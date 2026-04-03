from utils.db import Db
from utils.get_proxy import get_proxy_dicts
import json
import re
import logging
from curl_cffi import requests as curl_requests
from curl_cffi.requests import Session as CurlSession
from curl_cffi.requests.exceptions import ProxyError, RequestException
from collections import deque
import random
from threading import Condition, Semaphore
from concurrent.futures import ThreadPoolExecutor
import os
from dotenv import load_dotenv
from bs4 import BeautifulSoup

load_dotenv(override=True)

class ProxyPool:
    def __init__(self, configs: list[dict]):
        if not configs:
            raise Exception("The proxy list is empty.")
        self._configs = configs
        self._cond = Condition()
        order = list(range(len(configs)))
        random.shuffle(order)
        self._queue = deque(order)

    def acquire(self) -> int:
        with self._cond:
            while not self._queue:
                self._cond.wait()
            return self._queue.popleft()

    def release(self, proxy_id: int):
        with self._cond:
            self._queue.append(proxy_id)
            self._cond.notify()

    def config(self, proxy_id: int) -> dict:
        return self._configs[proxy_id]


def _looks_like_proxy_request_error(msg: str) -> bool:
    m = msg.lower()
    needles = (
        "proxy",
        "tunnel",
        "unable to connect",
        "connection refused",
        "err_tunnel",
        "err_proxy",
        "socks",
        "407",
        "502",
        "503",
    )
    return any(n in m for n in needles)


class ThreadsPageContent(Db):
    def __init__(self):
        super().__init__()

    def run(self):
        threads_count = int(os.getenv("THREADS_COUNT", "2"))
        proxy_pool = ProxyPool(get_proxy_dicts())

        max_workers = max(1, threads_count)
        slot = Semaphore(max_workers)

        def run_then_release(batch):
            try:
                self.run_thread(batch, proxy_pool)
            except Exception as ex:
                logging.exception(ex)
            finally:
                slot.release()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while True:
                batch = self.claim_airbnb_batch(100)
                if not batch:
                    break
                slot.acquire()
                executor.submit(run_then_release, batch)

    def run_thread(self, batch: list[tuple[int, str]], proxy_pool: ProxyPool) -> None:
        GetPageContent().run(batch, proxy_pool)


class GetPageContent(Db):
    def __init__(self):
        super().__init__()

    def run(self, rows: list[tuple[int, str]], proxy_pool: ProxyPool) -> None:
        if not rows:
            return
        proxy_id = proxy_pool.acquire()
        session = curl_requests.Session(impersonate="chrome")
        session.proxies = proxy_pool.config(proxy_id)
        try:
            for row_id, link in rows:
                html = self.get_content(session, row_id, link)
                if html is None:
                    continue
                root = self.parse_json_from_html(html)
                if root is None:
                    self.set_airbnb_status(row_id, 0)
                    continue
                data = self.get_data(root)
                if not self.update_data(data, row_id):
                    self.set_airbnb_status(row_id, 3)
                    continue
                print('updated data id=', row_id)
        finally:
            proxy_pool.release(proxy_id)
            self.close_connection()

    def parse_json_from_html(self, html: str) -> dict | None:
        soup = BeautifulSoup(html, "html.parser")
        tag = soup.find("script", id="data-deferred-state-0", type="application/json")
        if not tag:
            for t in soup.find_all("script", type="application/json"):
                tid = t.get("id") or ""
                if tid.startswith("data-deferred-state-"):
                    tag = t
                    break
        if not tag:
            return None
        text = (tag.string or tag.get_text() or "").strip()
        if not text:
            return None
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None

    def get_content(self, session: CurlSession, row_id: int, link: str) -> str | None:
        try:
            resp = session.get(link, timeout=20)
        except ProxyError as ex:
            logging.warning("proxy error id=%s: %s", row_id, ex)
            self.set_airbnb_status(row_id, 0)
            return None
        except RequestException as ex:
            logging.warning("request error id=%s: %s", row_id, ex)
            st = 0 if _looks_like_proxy_request_error(str(ex)) else 2
            self.set_airbnb_status(row_id, st)
            return None
        except Exception:
            logging.exception("fetch id=%s", row_id)
            self.set_airbnb_status(row_id, 2)
            return None

        if resp.status_code != 200 or not (resp.text or "").strip():
            self.set_airbnb_status(row_id, 2)
            return None

        return resp.text

    def update_data(self, data: dict, row_id: int) -> bool:
        columns = (
            "title",
            "address",
            "latitude",
            "longitude",
            "number_guests",
            "number_reviews",
            "average_reviews",
            "email",
            "phone",
            "business_name",
            "owner_name",
            "country",
            "status",
        )
        mapped: dict = {}
        for col in columns:
            if col not in data:
                continue
            value = data[col]
            if value in ("", None):
                continue
            if col == "average_reviews":
                try:
                    mapped[col] = float(value)
                except (TypeError, ValueError):
                    continue
            elif col in ("number_guests", "number_reviews", "status"):
                try:
                    mapped[col] = int(value)
                except (TypeError, ValueError):
                    continue
            elif col in ("latitude", "longitude"):
                try:
                    mapped[col] = float(value)
                except (TypeError, ValueError):
                    continue
            elif col == "business_name":
                mapped[col] = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
            else:
                mapped[col] = value

        if not mapped:
            return False

        set_columns = ", ".join(f"{c} = %s" for c in mapped.keys())
        set_values = tuple(mapped.values())
        sql = f"UPDATE airbnb SET {set_columns} WHERE id = %s"
        self.insert(sql, set_values + (row_id,))
        return True

    def _pdp_sections_list(self, src: dict) -> list | None:
        ncd = src.get("niobeClientData")
        if not isinstance(ncd, list) or not ncd:
            return None
        inner = ncd[0]
        if not isinstance(inner, list) or len(inner) < 2:
            return None
        payload = inner[1]
        if not isinstance(payload, dict):
            return None
        sections = (
            payload.get("data", {})
            .get("presentation", {})
            .get("stayProductDetailPage", {})
            .get("sections", {})
            .get("sections")
        )
        return sections if isinstance(sections, list) else None

    def get_section(self, sections: list | None, section_id: str) -> dict:
        if not sections:
            return {}
        for block in sections:
            if not isinstance(block, dict):
                continue
            if block.get("sectionId") == section_id or block.get("pluginPointId") == section_id:
                return block
        return {}

    def get_business(self, sections: list | None) -> tuple[str, str, str]:
        business_name = ""
        email = ""
        phone = ""
        block = self.get_section(sections, "PROFESSIONAL_HOST_DETAILS_MODAL")
        sec = block.get("section") or {}
        items = sec.get("items")
        if not items or not isinstance(items, list):
            return business_name, email, phone
        html_text = (items[0].get("html") or {}).get("htmlText")
        if not html_text:
            return business_name, email, phone
        mb = re.search(r"Business name:\s*([^<\n]+)", html_text, re.I)
        if mb:
            business_name = mb.group(1).strip()
        email_pattern = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
        lines = re.split(r"<br\s*/?>", html_text)
        try:
            for line in lines:
                line = re.sub(r"<[^>]+>", " ", line)
                line = line.strip()
                if not business_name and line:
                    business_name = line
                if not email:
                    match = email_pattern.search(line)
                    if match:
                        email = match.group()
                if not phone and "number" not in line.lower():
                    cleaned_line = re.sub(r"\D", "", line)
                    if len(cleaned_line) >= 7:
                        phone = line
        except Exception as ex:
            logging.warning("get_business parse: %s", ex)
        if not email:
            em = email_pattern.search(html_text)
            if em:
                email = em.group()
        if not phone:
            mp = re.search(r"Phone:\s*([^<\n]+)", html_text, re.I)
            if mp:
                phone = mp.group(1).strip()
        return business_name, email, phone

    def _country_from_breadcrumbs(self, sections: list | None) -> str:
        block = self.get_section(sections, "SEO_LINKS_DEFAULT")
        sec = block.get("section") or {}
        crumbs = sec.get("breadcrumbs")
        if isinstance(crumbs, list) and len(crumbs) >= 2:
            t = crumbs[1].get("title")
            return str(t).strip() if t else ""
        return ""

    def _collect_stay_embed_data(self, sections: list | None) -> dict:
        if not sections:
            return {}
        for block in sections:
            sec = block.get("section")
            if not isinstance(sec, dict):
                continue
            for node in (sec, sec.get("shareSave") or {}):
                ed = node.get("embedData")
                if isinstance(ed, dict) and (
                    ed.get("__typename") == "StayEmbedData"
                    or ed.get("starRating") is not None
                    or ed.get("reviewCount") is not None
                ):
                    return ed
        return {}

    def get_data(self, src: dict) -> dict:
        sections = self._pdp_sections_list(src)
        title = ""
        address = ""
        latitude = None
        longitude = None
        number_guests = None
        number_reviews = None
        average_reviews = None
        owner_name = ""
        country = ""
        email = ""
        phone = ""
        business_name = ""

        if sections:
            title_block = self.get_section(sections, "TITLE_DEFAULT")
            title_sec = title_block.get("section") or {}
            share = title_sec.get("shareSave") or {}
            embed = (
                title_sec.get("embedData")
                or share.get("embedData")
                or {}
            )
            sharing = (
                title_sec.get("sharingConfig")
                or share.get("sharingConfig")
                or {}
            )
            title = (
                title_sec.get("listingTitle")
                or title_sec.get("title")
                or embed.get("name")
                or ""
            )
            number_guests = embed.get("personCapacity")
            if number_guests is None:
                number_guests = sharing.get("personCapacity")
            number_reviews = embed.get("reviewCount")
            if number_reviews is None:
                number_reviews = sharing.get("reviewCount")
            average_reviews = embed.get("starRating")
            if average_reviews is None:
                average_reviews = sharing.get("starRating")
            if average_reviews is None and isinstance(sharing.get("title"), str):
                m = re.search(r"★\s*([\d.]+)", sharing["title"])
                if m:
                    try:
                        average_reviews = float(m.group(1))
                    except ValueError:
                        pass

            if not embed or average_reviews is None or number_guests is None:
                alt = self._collect_stay_embed_data(sections)
                if alt:
                    embed = {**alt, **embed}
                    if number_guests is None:
                        number_guests = embed.get("personCapacity")
                    if number_reviews is None:
                        number_reviews = embed.get("reviewCount")
                    if average_reviews is None:
                        average_reviews = embed.get("starRating")
                    if not title:
                        title = embed.get("name") or ""

            loc_block = self.get_section(sections, "LOCATION_DEFAULT")
            loc_sec = loc_block.get("section") or {}
            address = loc_sec.get("subtitle") or ""
            latitude = loc_sec.get("lat")
            longitude = loc_sec.get("lng")

            host_block = self.get_section(sections, "MEET_YOUR_HOST")
            host_sec = host_block.get("section") or {}
            card = host_sec.get("cardData") or {}
            owner_name = card.get("name") or ""

            business_name, email, phone = self.get_business(sections)
            country = self._country_from_breadcrumbs(sections)

        if isinstance(phone, str) and phone.lower().startswith("phone:"):
            phone = phone.split(":", 1)[1].strip()

        status = 1 if (title and str(title).strip()) else 0

        return {
            "title": title,
            "address": address,
            "average_reviews": average_reviews,
            "email": email,
            "phone": phone,
            "latitude": latitude,
            "longitude": longitude,
            "number_guests": number_guests,
            "number_reviews": number_reviews,
            "business_name": business_name,
            "owner_name": owner_name,
            "country": country,
            "status": status,
        }
