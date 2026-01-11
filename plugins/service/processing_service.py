from __future__ import annotations

import os
import shutil
import threading
import requests
import mimetypes
import time
import inspect
import asyncio
from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING

from plugins.drive import GoogleDrive
from plugins.gphotos import GPhotos
from plugins.registry import get_enabled_uploaders
from utils.logging_config import get_modern_logger

try:
    from api.common import simple_encrypt, simple_decrypt  # type: ignore
except Exception:
    from api.common import simple_encrypt  # type: ignore
    simple_decrypt = None  # type: ignore

from api.mysqldbreq import mysql_status_client

if TYPE_CHECKING:
    from queue import Queue
    from plugins.database import Database
    from queueing.redis_queue import RedisJobQueue

TaskArgs = Tuple[str, str, Optional[Dict[str, Any]], bool]


def _detect_filetype(filename: str) -> str:
    name = (filename or "").strip()
    if not name:
        return ""
    ext = os.path.splitext(name)[1].lower().lstrip(".")
    if ext:
        return ext
    mt, _ = mimetypes.guess_type(name)
    return mt or ""


def _now_ms() -> int:
    return int(time.time() * 1000)


class ProcessingService:
    def __init__(self, database: "Database", redis_queue: "RedisJobQueue") -> None:
        self.db = database
        self.redis_queue = redis_queue
        self.queue_lock = threading.Lock()
        self.pending_tasks: set[str] = set()
        self.active_tasks = 0
        self.task_queue: "Queue[TaskArgs]" = self._create_task_queue()
        self.logger = get_modern_logger("ProcessingService")

        self.upload_join_timeout = int(os.getenv("UPLOAD_JOIN_TIMEOUT", "1200"))
        self.viking_join_timeout = int(os.getenv("VIKING_JOIN_TIMEOUT", "1200"))

        self._results_lock = threading.Lock()

    def _create_task_queue(self) -> "Queue[TaskArgs]":
        if TYPE_CHECKING:
            from queue import Queue
            return Queue()
        import queue
        return queue.Queue()

    def _set_result(self, upload_results: Dict[str, str], key: str, value: Any) -> None:
        with self._results_lock:
            upload_results[key] = "" if value is None else str(value)

    def _safe_db_update(self, token: str, is_reupload: bool, **fields: Any) -> bool:
        update_method = self.db.reupload_link if is_reupload else self.db.update_link
        try:
            update_method(token, **fields)
            self.logger.info(f"✅ DB update ok | token={token} | fields={fields}")
            return True
        except TypeError as exc:
            self.logger.error(f"❌ DB update TypeError | token={token} | fields={fields} | err={exc}")
            return False
        except Exception as exc:
            self.logger.error(f"❌ DB update failed | token={token} | fields={fields} | err={exc}")
            return False

    def _safe_mysql_instant_update(self, token: str, **fields: Any) -> bool:
        try:
            fn = mysql_status_client.update_instant_fields
            sig = inspect.signature(fn)
            params = sig.parameters
            accepts_kwargs = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())

            if not accepts_kwargs:
                allowed = set(params.keys())
                mapped: Dict[str, Any] = {}
                alias_map = {
                    "pixeldrain_id": ["pixeldrain_id", "pixeldrain"],
                    "buzzheavier_id": ["buzzheavier_id", "buzzheavier"],
                    "viking_id": ["viking_id", "viking"],
                    "filetype": ["filetype", "mime", "mime_type"],
                    "gphotos_id": ["gphotos_id", "media_key", "mediaId", "media_key_id"],
                    "gp_id": ["gp_id", "gpid"],
                    "media_key": ["media_key", "gphotos_id"],
                }
                for k, v in fields.items():
                    if k in alias_map:
                        for cand in alias_map[k]:
                            if cand in allowed:
                                mapped[cand] = v
                                break
                    elif k in allowed:
                        mapped[k] = v
                fields_to_send = mapped
            else:
                fields_to_send = fields

            if not fields_to_send:
                self.logger.info(f"ℹ️ MySQL instant skipped (no supported fields) | token={token} | fields={fields}")
                return False

            fn(token=token, **fields_to_send)
            self.logger.info(f"✅ MySQL instant update ok | token={token} | fields={fields_to_send}")
            return True

        except TypeError as exc:
            self.logger.error(f"❌ MySQL instant TypeError | token={token} | fields={fields} | err={exc}")
            return False
        except Exception as exc:
            self.logger.error(f"❌ MySQL instant update failed | token={token} | fields={fields} | err={exc}")
            return False

    def process_file(self, token: str, file_id: str, file_info: Optional[Dict[str, Any]], is_reupload: bool = False) -> None:
        proc_label = "Reupload Process" if is_reupload else "File Process"
        self.logger.process_start(proc_label, token)
        start_ms = _now_ms()

        upload_results: dict[str, str] = {}
        local_path: Optional[str] = None
        filename: str = ""
        filesize: Any = None

        try:
            with self.queue_lock:
                self.active_tasks += 1
                self.pending_tasks.add(token)

            self.logger.info(
                f"🧾 Job start | token={token} | is_reupload={is_reupload} | file_id={file_id} | "
                f"active={self.active_tasks}"
            )

            drive = GoogleDrive()

            if file_info is None:
                link_data = self.db.get_link(token)
                if link_data:
                    file_info = {
                        "filename": link_data.get("filename"),
                        "filesize": link_data.get("filesize"),
                        "filetype": link_data.get("filetype", ""),
                        "file_id": file_id,
                    }
                    self.logger.info(
                        f"ℹ️ Using existing file info | token={token} | filename={file_info.get('filename')} | "
                        f"filetype={file_info.get('filetype')}"
                    )
                else:
                    self.logger.warning(f"⚠️ No DB record for token={token}. Fetching file info from Drive.")
                    file_info = drive.get_file_info(file_id)

            local_path, filename, filesize = drive.download_file(file_id, token, file_info=file_info, job_type="process_file")
            self.logger.info(
                f"⬇️ Downloaded | token={token} | file={filename} | size={filesize} | path={local_path}"
            )

            detected_type = (file_info or {}).get("filetype") or _detect_filetype(filename)
            if detected_type:
                self._safe_db_update(token, is_reupload, filetype=detected_type)
                self._safe_mysql_instant_update(token, filetype=detected_type)
                self.logger.info(f"🧩 Filetype resolved | token={token} | filetype={detected_type}")
            else:
                self.logger.warning(f"⚠️ Filetype empty | token={token} | file={filename}")

            enable_gphotos = os.getenv("ENABLE_GPHOTOS", "true").lower() == "true"
            self.logger.info(
                f"⚙️ Upload settings | token={token} | ENABLE_GPHOTOS={enable_gphotos} | "
                f"join_timeout={self.upload_join_timeout}s | viking_timeout={self.viking_join_timeout}s"
            )

            viking_uploader = None
            pixeldrain_uploader = None
            buzz_uploader = None
            other_uploaders = []

            for uploader in get_enabled_uploaders():
                up_name = (getattr(uploader, "name", "") or "").upper()
                if up_name == "VIKINGFILE":
                    viking_uploader = uploader
                    continue
                id_field = getattr(uploader, "id_field", "") or ""
                if id_field == "pixeldrain_id" or "PIXELDRAIN" in up_name:
                    pixeldrain_uploader = uploader
                elif id_field == "buzzheavier_id" or "BUZZ" in up_name:
                    buzz_uploader = uploader
                else:
                    other_uploaders.append(uploader)

            self.logger.info(
                f"🧱 Upload plan | token={token} | gphotos={enable_gphotos} | "
                f"pixeldrain={bool(pixeldrain_uploader)} | buzz={bool(buzz_uploader)} | "
                f"others={len(other_uploaders)} | viking={bool(viking_uploader)}"
            )

            if enable_gphotos:
                self.logger.info(f"🚀 Step-1: Google Photos upload start | token={token}")
                self._upload_gphotos(local_path, token, is_reupload, upload_results)
                self.logger.info(
                    f"✅ Step-1 done | token={token} | gphotos_id_set={bool(upload_results.get('gphotos_id'))} | "
                    f"skipped={bool(upload_results.get('gphotos_skipped'))}"
                )
            else:
                self.logger.info("⏭️ Step-1 skipped | Google Photos disabled")

            if pixeldrain_uploader:
                self.logger.info(f"🚀 Step-2: PixelDrain upload start | token={token}")
                self._run_uploader_blocking(pixeldrain_uploader, local_path, token, is_reupload, upload_results)
                self.logger.info(
                    f"✅ Step-2 done | token={token} | pixeldrain_id_set={bool(upload_results.get('pixeldrain_id'))}"
                )
            else:
                self.logger.info("⏭️ Step-2 skipped | PixelDrain uploader not enabled")

            parallel_uploaders = []
            if buzz_uploader:
                parallel_uploaders.append(buzz_uploader)
            parallel_uploaders.extend(other_uploaders)

            if parallel_uploaders:
                self.logger.info(f"🚀 Step-3: Parallel upload start | count={len(parallel_uploaders)} | token={token}")
                threads: list[threading.Thread] = []
                started = _now_ms()

                for uploader in parallel_uploaders:
                    up_name = getattr(uploader, "name", "UPLOADER")
                    t = threading.Thread(
                        target=self._make_upload_runner(uploader, local_path, token, is_reupload, upload_results),
                        name=f"uploader:{up_name}:{token[:6]}",
                    )
                    t.daemon = True
                    t.start()
                    threads.append(t)

                for t in threads:
                    t.join(timeout=self.upload_join_timeout)

                alive = [t.name for t in threads if t.is_alive()]
                took = (_now_ms() - started) / 1000.0
                if alive:
                    self.logger.warning(
                        f"⏳ Step-3 timeout | token={token} | alive={len(alive)} | seconds={took:.2f} | threads={alive}"
                    )
                self.logger.info(f"✅ Step-3 done | token={token} | seconds={took:.2f}")
            else:
                self.logger.info("⏭️ Step-3 skipped | No other uploaders enabled")

            if viking_uploader:
                self.logger.info(f"🔄 Step-4: VikingFile upload start (last) | token={token}")
                t_vk = threading.Thread(
                    target=self._make_upload_runner(viking_uploader, local_path, token, is_reupload, upload_results),
                    name=f"uploader:VIKINGFILE:{token[:6]}",
                )
                t_vk.daemon = True
                t_vk.start()
                t_vk.join(timeout=self.viking_join_timeout)
                if t_vk.is_alive():
                    self.logger.warning(f"⏳ Viking upload still running after timeout | token={token}")
                else:
                    self.logger.info(f"✅ Step-4 done | token={token}")
            else:
                self.logger.info("⏭️ Step-4 skipped | Viking uploader not enabled")

            link_doc = self.db.get_link(token) or {}

            self.logger.info(
                f"📌 Final snapshot | token={token} | db_pixeldrain={bool(link_doc.get('pixeldrain_id'))} | "
                f"db_buzz={bool(link_doc.get('buzzheavier_id'))} | db_viking={bool(link_doc.get('viking_id'))} | "
                f"db_gphotos={bool(link_doc.get('gphotos_id'))}"
            )
            self.logger.info(f"📌 Upload results keys | token={token} | keys={list(upload_results.keys())}")

            required_fields: list[str] = []
            if enable_gphotos:
                required_fields.append("gphotos_id")
            if pixeldrain_uploader:
                required_fields.append("pixeldrain_id")
            if buzz_uploader:
                required_fields.append("buzzheavier_id")

            if required_fields:
                has_provider_success = any((upload_results.get(f) or link_doc.get(f)) for f in required_fields)
            else:
                has_provider_success = True

            if not has_provider_success:
                raise Exception("All enabled upload services failed: " + ", ".join(required_fields))

            if is_reupload:
                self.logger.process_complete("Reupload Process", token)
                self._safe_db_update(token, True, status="completed", error=None)
            else:
                self.logger.process_complete("File Process", token)
                self._safe_db_update(token, False, status="completed", error=None)

            self._update_status_success(token, file_id)

            took = (_now_ms() - start_ms) / 1000.0
            self.logger.info(
                f"🏁 Done | token={token} | seconds={took:.2f} | "
                f"gphotos={'YES' if (upload_results.get('gphotos_id') or link_doc.get('gphotos_id')) else ('SKIP' if upload_results.get('gphotos_skipped') else 'NO')} | "
                f"pixeldrain={'YES' if (upload_results.get('pixeldrain_id') or link_doc.get('pixeldrain_id')) else 'NO'} | "
                f"viking={'YES' if (upload_results.get('viking_id') or link_doc.get('viking_id')) else 'NO'}"
            )

        except Exception as error:
            self._handle_processing_error(token, file_id, error, is_reupload)
        finally:
            try:
                self.pending_tasks.discard(token)
                self._schedule_next_task()
            finally:
                try:
                    self._cleanup_download(token)
                except Exception:
                    pass

    def _run_uploader_blocking(self, uploader, local_path: str, token: str, is_reupload: bool, upload_results: Dict[str, str]) -> None:
        runner = self._make_upload_runner(uploader, local_path, token, is_reupload, upload_results)
        runner()

    def _maybe_await(self, value: Any) -> Any:
        if inspect.isawaitable(value):
            try:
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = None
                if loop and loop.is_running():
                    return asyncio.run_coroutine_threadsafe(value, loop).result(timeout=self.upload_join_timeout)
                return asyncio.run(value)
            except Exception:
                return value
        return value

    def _make_upload_runner(self, uploader, local_path: str, token: str, is_reupload: bool, upload_results: Dict[str, str]):
        def runner() -> None:
            up_name = getattr(uploader, "name", "UPLOADER")
            started = _now_ms()

            try:
                if up_name == "VIKINGFILE":
                    link_data = self.db.get_link(token) or {}
                    gphotos_id = link_data.get("gphotos_id")
                    drive_file_id = link_data.get("drive_id")
                    filename = link_data.get("filename") or os.path.basename(local_path)

                    gphotos_raw = gphotos_id
                    if gphotos_id and simple_decrypt is not None:
                        try:
                            gphotos_raw = simple_decrypt(gphotos_id)
                        except Exception:
                            gphotos_raw = gphotos_id

                    self.logger.upload_start("VikingFile", filename)

                    res = uploader.upload(
                        local_path,
                        token=token,
                        gphotos_id=gphotos_raw,
                        file_id=drive_file_id,
                        filename=filename,
                    )
                    file_id_value = self._maybe_await(res)
                    db_field = "viking_id"
                else:
                    filename = os.path.basename(local_path)
                    self.logger.upload_start(up_name, filename)

                    res = uploader.upload(local_path, token)
                    file_id_value = self._maybe_await(res)

                    db_field = getattr(uploader, "id_field", "") or ""
                    if not db_field:
                        db_field = f"{str(up_name).lower()}_id"

                file_id_value = "" if file_id_value is None else str(file_id_value).strip()
                self._set_result(upload_results, db_field, file_id_value)

                if not file_id_value:
                    raise Exception(f"{up_name} returned empty id for field={db_field}")

                self._safe_db_update(token, is_reupload, **{db_field: file_id_value})
                self._safe_mysql_instant_update(token, **{db_field: file_id_value})

                took = (_now_ms() - started) / 1000.0
                if up_name == "VIKINGFILE":
                    self.logger.upload_success("VikingFile", file_id_value)
                else:
                    self.logger.upload_success(up_name, file_id_value)

                self.logger.info(
                    f"✅ Upload done | {up_name} | token={token} | field={db_field} | id={file_id_value} | seconds={took:.2f}"
                )

            except Exception as exc:
                msg = str(exc)
                took = (_now_ms() - started) / 1000.0
                if up_name == "VIKINGFILE":
                    self.logger.upload_failed("VikingFile (optional)", msg)
                else:
                    self.logger.upload_failed(up_name, msg)

                self._set_result(upload_results, f"{str(up_name).lower()}_error", msg)
                self.logger.error(f"❌ Upload failed | {up_name} | token={token} | seconds={took:.2f} | err={msg}")

        return runner

    def _upload_gphotos(self, local_path: str, token: str, is_reupload: bool, upload_results: Dict[str, str]) -> None:
        started = _now_ms()
        try:
            gphotos = GPhotos()
            filename = os.path.basename(local_path)

            link_data = self.db.get_link(token) or {}
            db_filetype = (link_data.get("filetype") or "").strip()

            if not gphotos.is_video_file(local_path, db_filetype=db_filetype):
                self.logger.info(
                    f"⏭️ Google Photos skipped (non-video) | token={token} | filetype={db_filetype or 'unknown'} | file={filename}"
                )
                self._set_result(upload_results, "gphotos_skipped", db_filetype or "non-video")
                return

            self.logger.upload_start("Google Photos", filename)

            gphotos_id = gphotos.upload(local_path, token, db_filetype=db_filetype)
            if not gphotos_id:
                raise Exception("Google Photos returned empty id")

            encrypted_id = simple_encrypt(gphotos_id)

            gp_identity = os.getenv("GP_IDENTITY", "")
            encrypted_gp_id = simple_encrypt(gp_identity) if gp_identity else None

            self._safe_db_update(
                token,
                is_reupload,
                gphotos_id=encrypted_id,
                **({"gp_id": encrypted_gp_id} if encrypted_gp_id else {}),
            )

            self._safe_mysql_instant_update(
                token,
                media_key=encrypted_id,
                gphotos_id=encrypted_id,
                **({"gp_id": encrypted_gp_id} if encrypted_gp_id else {}),
            )

            self._set_result(upload_results, "gphotos_id", encrypted_id)
            if encrypted_gp_id:
                self._set_result(upload_results, "gp_id", encrypted_gp_id)

            self.logger.upload_success("Google Photos", gphotos_id)
            took = (_now_ms() - started) / 1000.0
            self.logger.info(f"✅ Google Photos saved | token={token} | seconds={took:.2f}")

        except Exception as exc:
            msg = str(exc)
            took = (_now_ms() - started) / 1000.0
            self.logger.upload_failed("Google Photos", msg)
            self._set_result(upload_results, "gphotos_error", msg)
            self.logger.error(f"❌ Google Photos failed | token={token} | seconds={took:.2f} | err={msg}")

    def _update_status_success(self, token: str, file_id: str) -> None:
        try:
            self.logger.info(f"🟢 Status update (success) via MySQL | token={token}")
            updated_via_mysql = mysql_status_client.update_status(
                token=token,
                status="success",
                message="Working",
                drive_url=file_id,
            )
            if updated_via_mysql:
                self.logger.info(f"✅ Status updated via MySQL | token={token}")
                return
        except Exception as exc:
            self.logger.error(f"❌ MySQL status success failed | token={token} | err={exc}")

        api_url = os.getenv("STATUS_API_URL")
        api_key = os.getenv("STATUS_API_KEY")
        if not api_url or not api_key:
            return

        try:
            headers = {"Content-Type": "application/json", "X-API-Key": api_key}
            payload = {"token": token, "status": "success", "message": "Working", "drive_url": file_id}
            self.logger.info(f"🟢 Status API post (success) | token={token}")
            response = requests.post(api_url, json=payload, headers=headers, verify=False, timeout=20)
            response.raise_for_status()
            self.logger.info(f"✅ Status API updated | token={token}")
        except Exception as exc:
            self.logger.error(f"❌ Status API success failed | token={token} | err={exc}")

    def _handle_processing_error(self, token: str, file_id: str, error: Exception, is_reupload: bool) -> None:
        error_message = str(error)
        if is_reupload:
            self.logger.error(f"❌ Reupload failed | token={token} | err={error_message}")
            self._safe_db_update(token, True, status="failed", error=error_message)
        else:
            self.logger.error(f"❌ File process failed | token={token} | err={error_message}")
            self._safe_db_update(token, False, status="failed", error=error_message)
        self._update_status_error(token, file_id, error_message)

    def _update_status_error(self, token: str, file_id: str, error_message: str) -> None:
        try:
            self.logger.info(f"🔴 Status update (error) via MySQL | token={token}")
            updated_via_mysql = mysql_status_client.update_status(
                token=token,
                status="error",
                message=error_message,
                drive_url=file_id,
            )
            if updated_via_mysql:
                self.logger.info(f"✅ Status updated via MySQL (error) | token={token}")
                return
        except Exception as exc:
            self.logger.error(f"❌ MySQL status error failed | token={token} | err={exc}")

    def _schedule_next_task(self) -> None:
        with self.queue_lock:
            self.active_tasks -= 1
            if self.task_queue.empty() or self.active_tasks < 0:
                self.active_tasks = max(0, self.active_tasks)
                return
            next_task = self.task_queue.get()
            self.logger.info(
                f"➡️ Scheduling next queued task | token={next_task[0]} | active={self.active_tasks} | queued={self.task_queue.qsize()}"
            )
            thread = threading.Thread(target=self.process_file, args=next_task, name=f"process:{next_task[0][:6]}")
            thread.daemon = True
            thread.start()

    def _cleanup_download(self, token: str) -> None:
        base = os.path.join(os.getcwd(), "downloads")
        paths = [
            os.path.join(base, token),
            os.path.join(base, "recovery", token),
        ]
        for p in paths:
            if os.path.exists(p):
                try:
                    shutil.rmtree(p)
                    self.logger.info(f"🧹 Cleaned downloads dir | token={token} | dir={p}")
                except Exception as exc:
                    self.logger.error(f"❌ Cleanup failed | token={token} | dir={p} | err={exc}")

    def enqueue_local_fallback(self, task: TaskArgs) -> None:
        with self.queue_lock:
            if self.active_tasks <= 0:
                self.logger.info(f"▶️ Starting task immediately | token={task[0]} | active={self.active_tasks}")
                thread = threading.Thread(target=self.process_file, args=task, name=f"process:{task[0][:6]}")
                thread.daemon = True
                thread.start()
            else:
                self.task_queue.put(task)
                self.logger.info(
                    f"🧾 Queued task | token={task[0]} | active={self.active_tasks} | queued={self.task_queue.qsize()}"
                )
