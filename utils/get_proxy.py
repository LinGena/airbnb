
import os
import requests
from datetime import datetime
from requests.exceptions import ProxyError
from dotenv import load_dotenv

load_dotenv(override=True)

def get_proxy(id: int = None) -> dict:
    all_proxies = get_list_proxies()
    if not all_proxies:
        raise Exception("The proxies list is empty or proxies are not United Kingdom.")
    proxy_strings = [proxy_to_string(proxy) for proxy in all_proxies]
    if len(proxy_strings) > 0:
        if id is not None:
            return { 
                'http': proxy_strings[id],
                'https': proxy_strings[id]
            } 
        return proxy_strings
    return None

def get_list_proxies() -> list | None:
    user_agent = "Mozilla/5.0 (platform; rv:geckoversion) Gecko/geckotrail Firefox/firefoxversion"
    ip_royal_headers = {
        'User-Agent': user_agent,
        'X-Access-Token': os.getenv("IPROYAL_API_KEY")
    }
    page = 1
    proxies = []
    while True:
        try:
            response = requests.get('https://apid.iproyal.com/v1/reseller/orders', headers=ip_royal_headers,
                                    params={'per_page': 1000, 'status': 'confirmed', 'product_id': int(3), 'page': 1},
                                    verify=True)
            data = response.json()
            response.close()
            if data.get('data'):
                for item in data['data']:
                    if item.get('location') == "United Kingdom":
                        proxies.extend(format_data(item))
                if page >= data['meta']['last_page']:
                    break
                page += 1
            else:
                raise Exception("Error occurred while getting proxies." + " " + str(data))
        except ProxyError:
            print('proxy error')
            return None
    return proxies

def proxy_to_string(proxy: dict):
    return f"http://{proxy.get('login')}:{proxy.get('password')}@{proxy.get('ip')}:{proxy.get('port_http')}"

def format_data(data):
    formatted_data = []
    for proxy in data['proxy_data']['proxies']:
        formatted_data.append({
            'id': str(data['id']),
            'order_id': str(data['id']),
            'basket_id': '',
            'ip': proxy['ip'],
            'ip_only': proxy['ip'],
            'protocol': 'HTTP',
            'port_socks': 12324,
            'port_http': 12323,
            'login': proxy['username'],
            'password': proxy['password'],
            'auth_ip': '',
            'rotation': None,
            'link_reboot': '#',
            'country': data['location'],
            'country_alpha3': '',
            'status': 'Активные' if data['status'] == 'confirmed' else 'Неактивные',
            'status_type': 'ACTIVE' if data['status'] == 'confirmed' else 'INACTIVE',
            'can_prolong': True,
            'date_start': '',
            'date_end': datetime.strptime(data['expire_date'], '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y'),
            'comment': data['note'] if data['note'] else '',
            'auto_renew': 'N',
            'auto_renew_period': ''
        })
    return formatted_data