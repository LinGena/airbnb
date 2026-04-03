from mysql.connector import connect, Error
from dotenv import load_dotenv
import os
import time


load_dotenv(override=True)


def _db_connection_kwargs():
    return {
        "host": os.getenv("DB_HOST", ""),
        "port": int(os.getenv("DB_PORT", 3306)),
        "user": os.getenv("DB_USER", ""),
        "password": os.getenv("DB_PASS", ""),
        "database": os.getenv("DB_NAME", ""),
    }


class Db():
    def __init__(self):
        self.connecting()

    def __del__(self):
        self.close_connection()

    def connecting(self, max_retries=10, delay=5) -> None:    
        for i in range(max_retries):
            try:
                self.connection = connect(**_db_connection_kwargs())
                self.cursor = self.connection.cursor()
                return
            except Error as e:
                time.sleep(delay)
        raise Exception("Could not connect to the database after multiple attempts")

    def insert(self, sql, params):
        self.cursor.execute(sql, params)
        self.connection.commit()

    def insert_many(self, sql: str, params_list: list, batch_size: int = 500) -> None:
        for i in range(0, len(params_list), batch_size):
            batch = params_list[i : i + batch_size]
            self.cursor.executemany(sql, batch)
            self.connection.commit()

    def select(self, sql: str, params=None, with_column_names=False) -> list:
        if params is not None:
            self.cursor.execute(sql, params)
        else:
            self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        if with_column_names:
            column_names = [desc[0] for desc in self.cursor.description]
            return [dict(zip(column_names, row)) for row in rows]
        return rows
        
    def close_connection(self):
        self.connection.close()

    def getMaxId(self, id_name: str, baseName: str):
        sql = f"SELECT MAX({id_name}) FROM {baseName}"
        maxid = self.select(sql)
        if maxid[0][0]!=None:
            return int(maxid[0][0])+1
        else:
            return 1

    def claim_airbnb_batch(self, limit: int = 100) -> list[tuple]:
        try:
            self.connection.autocommit = False
            self.cursor.execute(
                """SELECT id, link FROM airbnb
                   WHERE (status IS NULL OR status = 0)
                   ORDER BY RAND() LIMIT %s FOR UPDATE""",
                (limit,),
            )
            rows = self.cursor.fetchall()
            if not rows:
                self.connection.commit()
                return []
            ids = [r[0] for r in rows]
            placeholders = ",".join(["%s"] * len(ids))
            self.cursor.execute(
                f"UPDATE airbnb SET status = 10 WHERE id IN ({placeholders})",
                ids,
            )
            self.connection.commit()
            return list(rows)
        except Exception:
            self.connection.rollback()
            raise
        finally:
            self.connection.autocommit = True

    def set_airbnb_status(self, row_id: int, status: int) -> None:
        self.insert("UPDATE airbnb SET status = %s WHERE id = %s", (status, row_id))


_AIRBNB_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS `airbnb` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `link` varchar(250) UNIQUE,
  `title` text,
  `latitude` double DEFAULT NULL,
  `longitude` double DEFAULT NULL,
  `number_guests` int DEFAULT NULL,
  `number_reviews` int DEFAULT NULL,
  `average_reviews` float DEFAULT NULL,
  `address` text,
  `owner_name` text,
  `business_name` text,
  `email` text,
  `phone` text,
  `country` varchar(50) DEFAULT NULL,
  `status` int DEFAULT NULL,
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_country` (`country`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


class DbSchema:
    def ensure_airbnb_table(self) -> None:
        connection = connect(**_db_connection_kwargs())
        cursor = None
        try:
            cursor = connection.cursor()
            cursor.execute(_AIRBNB_CREATE_SQL)
            connection.commit()
        finally:
            if cursor is not None:
                cursor.close()
            connection.close()

