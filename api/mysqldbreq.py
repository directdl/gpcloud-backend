import threading
from typing import Optional, Dict, Any

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

    def _get_connection(self):
        return pymysql.connect(
            host=self._db_config['host'],
            user=self._db_config['user'],
            password=self._db_config['password'],
            database=self._db_config['database'],
            port=self._db_config['port'],
            charset=self._db_config['charset'],
            cursorclass=self._db_config['cursorclass'],
            autocommit=self._db_config.get('autocommit', True),
        )

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
        """Update status/message/drive_url (and optionally filename/filesize) for an existing token.
        Returns True when the row exists (even if values are identical and 0 rows affected)."""
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
                    # If no rows changed, it may still be a success if the row exists but values are identical
                    cur.execute("SELECT 1 FROM filecloud WHERE token = %s LIMIT 1", (token,))
                    exists = cur.fetchone() is not None
                    return exists

    def upsert_status(self, token: str, status: str, message: str, drive_url: Optional[str]) -> None:
        """Create or update token row. Creates row if not found, else updates."""
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


# Module-level singleton (optional use)
mysql_status_client = MySQLStatusClient()



