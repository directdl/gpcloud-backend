import os
import threading
from typing import Optional, Dict, Any, Set

import pymysql


DB_CONFIG: Dict[str, Any] = {
    'host': '146.103.41.168',
    'database': 'gpcloud',
    'user': 'gpcloud',
    'password': '01759335737',
    'charset': 'utf8mb4',
    'port': 8860,
    'cursorclass': pymysql.cursors.DictCursor,
    'autocommit': True,
}

class MySQLStatusClient:
    """Thin MySQL client for updating and reading filecloud statuses directly."""

    def __init__(self, db_config: Optional[Dict[str, Any]] = None) -> None:
        self._db_config = db_config or DB_CONFIG
        self._lock = threading.Lock()
        self._columns_cache: Optional[Set[str]] = None

    def _get_connection(self):
        return pymysql.connect(
            host=self._db_config["host"],
            user=self._db_config["user"],
            password=self._db_config["password"],
            database=self._db_config["database"],
            port=self._db_config["port"],
            charset=self._db_config["charset"],
            cursorclass=self._db_config["cursorclass"],
            autocommit=self._db_config.get("autocommit", True),
        )

    def _get_filecloud_columns(self) -> Set[str]:
        if self._columns_cache is not None:
            return self._columns_cache

        sql = """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'filecloud'
        """
        cols: Set[str] = set()
        with self._lock:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (self._db_config["database"],))
                    rows = cur.fetchall() or []
                    for r in rows:
                        name = (r.get("COLUMN_NAME") or "").strip()
                        if name:
                            cols.add(name)

        self._columns_cache = cols
        return cols

    def token_exists(self, token: str) -> bool:
        sql = "SELECT id FROM filecloud WHERE token = %s LIMIT 1"
        with self._lock:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (token,))
                    row = cur.fetchone()
                    return bool(row)

    def update_status(
        self,
        token: str,
        status: str,
        message: str,
        drive_url: Optional[str],
        filename: Optional[str] = None,
        filesize: Optional[str] = None,
    ) -> bool:
        set_parts = ["status = %s", "message = %s", "drive_url = %s"]
        params = [status, message, drive_url]
        if filename is not None:
            set_parts.append("filename = %s")
            params.append(filename)
        if filesize is not None:
            set_parts.append("filesize = %s")
            params.append(filesize)
        sql = f"UPDATE filecloud SET {', '.join(set_parts)} WHERE token = %s"
        with self._lock:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    rows = cur.execute(sql, (*params, token))
                    if rows > 0:
                        return True
                    cur.execute("SELECT 1 FROM filecloud WHERE token = %s LIMIT 1", (token,))
                    return cur.fetchone() is not None

    def update_instant_fields(
        self,
        token: str,
        media_key: Optional[str] = None,
        gphotos_id: Optional[str] = None,
        gp_id: Optional[str] = None,
        filetype: Optional[str] = None,
        pixeldrain_id: Optional[str] = None,
        buzzheavier_id: Optional[str] = None,
        viking_id: Optional[str] = None,
    ) -> bool:
        columns = self._get_filecloud_columns()

        pairs: Dict[str, Optional[str]] = {
            "media_key": media_key,
            "gphotos_id": gphotos_id,
            "gp_id": gp_id,
            "filetype": filetype,
            "pixeldrain_id": pixeldrain_id,
            "buzzheavier_id": buzzheavier_id,
            "viking_id": viking_id,
        }

        set_parts = []
        params = []

        for col, val in pairs.items():
            if val is None:
                continue
            if col not in columns:
                continue
            set_parts.append(f"{col} = %s")
            params.append(val)

        if not set_parts:
            return False

        sql = f"UPDATE filecloud SET {', '.join(set_parts)} WHERE token = %s"
        with self._lock:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    rows = cur.execute(sql, (*params, token))
                    if rows > 0:
                        return True
                    cur.execute("SELECT 1 FROM filecloud WHERE token = %s LIMIT 1", (token,))
                    return cur.fetchone() is not None

    def upsert_status(self, token: str, status: str, message: str, drive_url: Optional[str]) -> None:
        with self._lock:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM filecloud WHERE token = %s LIMIT 1", (token,))
                    row = cur.fetchone()
                    if row:
                        cur.execute(
                            """
                            UPDATE filecloud
                            SET status = %s, message = %s, drive_url = %s
                            WHERE token = %s
                            """,
                            (status, message, drive_url, token),
                        )
                    else:
                        cur.execute(
                            """
                            INSERT INTO filecloud (token, status, message, drive_url, created)
                            VALUES (%s, %s, %s, %s, NOW())
                            """,
                            (token, status, message, drive_url),
                        )


mysql_status_client = MySQLStatusClient()
