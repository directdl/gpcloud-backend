from flask import Blueprint, jsonify, request, Response
import secrets
import threading
import os
import json
import signal
import atexit
import hashlib
import base64
import urllib3
from typing import Any, Dict, Optional, Union

from plugins.database_factory import get_database
from utils.validators import ensure_filesize_within_limit
from utils.statuses import (
    STATUS_COMPLETED,
    STATUS_ERROR,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_SUCCESS,
)
from queueing.redis_queue import RedisJobQueue

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

api = Blueprint("api", __name__)

db = None
_db_lock = threading.Lock()

redis_queue = RedisJobQueue()

shutdown_event = threading.Event()

ResponseData = Dict[str, Any]
ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789"

_api_keys_cache: Dict[str, Dict[str, Any]] = {}
_api_keys_mtime: float = -1.0
_api_keys_lock = threading.Lock()


def _ensure_db() -> None:
    global db
    if db is not None:
        return
    with _db_lock:
        if db is None:
            db = get_database()


def init_routes_services():
    _ensure_db()


def signal_handler(signum: int, frame: Any) -> None:
    shutdown_event.set()
    os._exit(0)


def register_signal_handlers() -> None:
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)


atexit.register(lambda: None)


def gen_token(length: int = 8) -> str:
    return "".join(secrets.choice(ALPHABET) for _ in range(length))


def _convert_filesize_to_raw_size(filesize_str: Optional[str]) -> Optional[int]:
    if not filesize_str:
        return None
    size_str = str(filesize_str).upper().strip()
    try:
        if "TB" in size_str:
            v = float(size_str.replace("TB", "").strip())
            return int(v * 1024 * 1024 * 1024 * 1024)
        if "GB" in size_str:
            v = float(size_str.replace("GB", "").strip())
            return int(v * 1024 * 1024 * 1024)
        if "MB" in size_str:
            v = float(size_str.replace("MB", "").strip())
            return int(v * 1024 * 1024)
        if "KB" in size_str:
            v = float(size_str.replace("KB", "").strip())
            return int(v * 1024)
        if size_str.endswith("B"):
            return int(size_str.replace("B", "").strip())
        return int(float(size_str))
    except (ValueError, TypeError):
        return None


def simple_encrypt(text: str) -> Optional[str]:
    if not text:
        return None
    secret = os.getenv("ENCRYPTION_SECRET", "mksbhai")
    hv = hashlib.md5((text + secret).encode()).hexdigest()[:8]
    result = hv + text
    encoded = base64.b64encode(result.encode()).decode()
    encoded = encoded.replace("+", "-").replace("/", "_").replace("=", "")
    return encoded


def _keys_file_path() -> str:
    return os.path.join(os.path.dirname(__file__), "keys.json")


def load_api_keys() -> Dict[str, Dict[str, Any]]:
    global _api_keys_cache, _api_keys_mtime
    keys_file = _keys_file_path()
    try:
        mtime = os.path.getmtime(keys_file)
    except OSError:
        return {}
    with _api_keys_lock:
        if _api_keys_cache and _api_keys_mtime == mtime:
            return _api_keys_cache
        try:
            with open(keys_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            users = data.get("users", [])
            _api_keys_cache = {u.get("api_key"): u for u in users if u.get("api_key")}
            _api_keys_mtime = mtime
            return _api_keys_cache
        except Exception:
            _api_keys_cache = {}
            _api_keys_mtime = mtime
            return {}


def validate_api_key(api_key: str) -> bool:
    api_keys = load_api_keys()
    user = api_keys.get(api_key)
    if not user:
        return False
    return bool(user.get("is_active"))


def _safe_error(msg: str, code: int = 500) -> tuple[Response, int]:
    return jsonify({"success": False, "error": msg}), code


def _extract_input_drive_id(drive: Any, drive_input: str) -> Optional[str]:
    val = (drive_input or "").strip()
    if not val:
        return None
    if "drive.google.com" in val:
        try:
            return drive.extract_file_id(val)
        except Exception:
            return None
    return val


def _check_interrupted_download(token: str) -> bool:
    download_dir = os.path.join(os.getcwd(), "downloads", token)
    return os.path.exists(download_dir)


def _recover_interrupted_task(token: str, drive_id: str) -> None:
    from plugins.drive import GoogleDrive

    _ensure_db()
    if db is None:
        return

    try:
        if redis_queue.redis.exists(redis_queue._get_job_key(token)):
            return

        link_data = db.get_link(token)
        if link_data and link_data.get("filename"):
            file_info = {
                "filename": link_data.get("filename"),
                "filesize": link_data.get("filesize"),
                "file_id": drive_id,
                "filetype": link_data.get("filetype", ""),
            }

            if file_info.get("filesize"):
                raw_size = _convert_filesize_to_raw_size(file_info["filesize"])
                if raw_size:
                    file_info["raw_size"] = raw_size
                    redis_queue.enqueue_process(token, drive_id, file_info, False)
                    return

            drive = GoogleDrive()
            file_info = drive.get_file_info(drive_id)
            redis_queue.enqueue_process(token, drive_id, file_info, False)
            return

        drive = GoogleDrive()
        file_info = drive.get_file_info(drive_id)
        redis_queue.enqueue_process(token, drive_id, file_info, False)

    except Exception as e:
        err_text = str(e)
        if db is not None:
            if "not found" in err_text.lower() or "no access" in err_text.lower():
                friendly = "File not found in Google Drive or access has been revoked"
            else:
                friendly = f"Recovery failed: {err_text}"
            db.update_link(token, status=STATUS_FAILED, error=friendly)


def recover_pending_tasks() -> None:
    _ensure_db()
    if db is None:
        return
    try:
        pending_iter = db.iter_pending_links(batch_size=int(os.getenv("RECOVERY_BATCH_SIZE", "200")))
        for link in pending_iter:
            token = link["token"]
            drive_id = link["drive_id"]

            if _check_interrupted_download(token):
                db.update_link(token, status=STATUS_FAILED, error="Task interrupted by server restart")
                continue

            _recover_interrupted_task(token, drive_id)
    except Exception:
        return


def init_recovery_on_startup() -> None:
    recover_pending_tasks()


@api.route("/<api_key>/reupload/<token>", methods=["GET"])
def reupload_file(api_key: str, token: str) -> Union[Response, tuple[Response, int]]:
    if os.getenv("ENABLE_PROCESS", "true").lower() != "true":
        return _safe_error("System processing is currently disabled", 503)

    _ensure_db()
    if db is None:
        return _safe_error("Database not initialized", 500)

    try:
        if not validate_api_key(api_key):
            return _safe_error("Invalid or inactive API key", 401)

        link_data = db.get_link(token)
        if not link_data:
            return _safe_error("Link not found", 404)

        file_id = link_data["drive_id"]
        file_info = {
            "filename": link_data.get("filename"),
            "filesize": link_data.get("filesize"),
            "filetype": link_data.get("filetype"),
            "file_id": link_data.get("drive_id"),
        }

        try:
            job_id = redis_queue.enqueue_process(token, file_id, file_info, True)
        except Exception:
            return _safe_error("Redis queue unavailable. Please retry later.", 503)

        return jsonify(
            {
                "success": True,
                "message": "Reupload started",
                "data": {
                    "token": token,
                    "filename": link_data.get("filename"),
                    "filesize": link_data.get("filesize"),
                    "status": STATUS_PENDING,
                    "check_url": f"/api/{api_key}/status/{token}",
                    "job_id": str(job_id),
                },
            }
        )
    except Exception as e:
        return _safe_error(str(e), 500)


def _generate_link_core(api_key: str, drive_input: str) -> Union[Response, tuple[Response, int]]:
    if os.getenv("ENABLE_PROCESS", "true").lower() != "true":
        return _safe_error("System processing is currently disabled", 503)

    _ensure_db()
    if db is None:
        return _safe_error("Database not initialized", 500)

    from plugins.drive import GoogleDrive

    try:
        if not validate_api_key(api_key):
            return _safe_error("Invalid or inactive API key", 401)

        drive = GoogleDrive()
        file_id = _extract_input_drive_id(drive, drive_input)
        if not file_id:
            return _safe_error("Invalid Google Drive link", 400)

        existing_link = db.find_link_by_drive_id(file_id)
        if existing_link:
            return jsonify(
                {
                    "success": True,
                    "message": "Existing link found",
                    "data": {
                        "token": existing_link["token"],
                        "filename": existing_link.get("filename"),
                        "filesize": existing_link.get("filesize"),
                        "status": existing_link["status"],
                        "check_url": f"/api/{api_key}/status/{existing_link['token']}",
                        "is_existing": True,
                    },
                }
            )

        file_info = drive.get_file_info(file_id)

        max_size = float(os.getenv("MAX_FILE_SIZE_GB", "6.5"))
        try:
            ensure_filesize_within_limit(file_info.get("filesize"), max_size_gb=max_size)
        except ValueError:
            fs = file_info.get("filesize")
            return (
                jsonify(
                    {
                        "success": False,
                        "error": f"File size ({fs}) exceeds maximum limit of {max_size}GB",
                        "max_size": f"{max_size}GB",
                        "file_size": fs,
                    }
                ),
                400,
            )

        token = None
        for _ in range(40):
            t = gen_token(8)
            if not db.token_exists(t):
                token = t
                break
        if not token:
            return _safe_error("token generation failed", 500)

        db.save_link(
            token=token,
            drive_id=file_id,
            status=STATUS_PENDING,
            filename=file_info["filename"],
            filesize=file_info["filesize"],
            filetype=file_info.get("filetype", ""),
        )

        try:
            job_id = redis_queue.enqueue_process(token, file_id, file_info, False)
        except Exception:
            db.update_link(token, status=STATUS_FAILED, error="Redis queue unavailable. Please retry later.")
            return _safe_error("Redis queue unavailable. Please retry later.", 503)

        return jsonify(
            {
                "success": True,
                "message": "Link generation started",
                "data": {
                    "token": token,
                    "filename": file_info["filename"],
                    "filesize": file_info["filesize"],
                    "status": STATUS_PENDING,
                    "check_url": f"/api/{api_key}/status/{token}",
                    "job_id": str(job_id),
                    "is_existing": False,
                },
            }
        )
    except Exception as e:
        return _safe_error(str(e), 500)


@api.route("/<api_key>/generate", methods=["POST"])
def generate_link_post(api_key: str) -> Union[Response, tuple[Response, int]]:
    body = request.get_json(silent=True) or {}
    drive_url = body.get("url") or body.get("drive_url") or body.get("drive") or ""
    return _generate_link_core(api_key, str(drive_url))


@api.route("/<api_key>/generate", methods=["GET"])
def generate_link_get(api_key: str) -> Union[Response, tuple[Response, int]]:
    drive_url = request.args.get("url", "") or request.args.get("drive_url", "") or ""
    return _generate_link_core(api_key, str(drive_url))


@api.route("/<api_key>/<path:drive_path>", methods=["GET"])
def generate_link(api_key: str, drive_path: str) -> Union[Response, tuple[Response, int]]:
    return _generate_link_core(api_key, drive_path)


@api.route("/<api_key>/status/<token>", methods=["GET"])
def check_status(api_key: str, token: str) -> Union[Response, tuple[Response, int]]:
    _ensure_db()
    if db is None:
        return _safe_error("Database not initialized", 500)

    try:
        if not validate_api_key(api_key):
            return _safe_error("Invalid or inactive API key", 401)

        link_data = db.get_link(token)
        if not link_data:
            return _safe_error("Link not found", 404)

        data: Dict[str, Any] = {
            "token": token,
            "status": link_data["status"],
            "filename": link_data.get("filename"),
            "filesize": link_data.get("filesize"),
            "filetype": link_data.get("filetype", ""),
        }

        if link_data.get("pixeldrain_id"):
            data["pixeldrain_url"] = f"https://pixeldrain.com/u/{link_data['pixeldrain_id']}"
        if link_data.get("buzzheavier_id"):
            data["buzzheavier_url"] = f"https://buzzheavier.com/{link_data['buzzheavier_id']}"
        if link_data.get("viking_id"):
            data["viking_url"] = f"https://vikingfile.com/f/{link_data['viking_id']}"
        if link_data.get("gphotos_id"):
            data["gphotos_id"] = link_data["gphotos_id"]
        if link_data.get("gp_id"):
            data["gp_id"] = link_data["gp_id"]

        if link_data["status"] == STATUS_FAILED:
            data["error"] = link_data.get("error") or "Processing failed"

        return jsonify({"success": True, "data": data})
    except Exception as e:
        return _safe_error(str(e), 500)
