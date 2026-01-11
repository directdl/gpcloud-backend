from __future__ import annotations

import os
import shutil
import threading
import requests
import mimetypes
from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING

from plugins.drive import GoogleDrive
from plugins.gphotos import GPhotos
from plugins.registry import get_enabled_uploaders
from utils.logging_config import get_modern_logger
from api.common import simple_encrypt
from api.mysqldbreq import mysql_status_client

if TYPE_CHECKING:  # pragma: no cover
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


class ProcessingService:
    def __init__(self, database: "Database", redis_queue: "RedisJobQueue") -> None:
        self.db = database
        self.redis_queue = redis_queue
        self.queue_lock = threading.Lock()
        self.pending_tasks: set[str] = set()
        self.active_tasks = 0
        self.task_queue: "Queue[TaskArgs]" = self._create_task_queue()
        self.logger = get_modern_logger("ProcessingService")

    def _create_task_queue(self) -> "Queue[TaskArgs]":
        if TYPE_CHECKING:  # pragma: no cover
            from queue import Queue
            return Queue()
        import queue
        return queue.Queue()

    def process_file(self, token: str, file_id: str, file_info: Optional[Dict[str, Any]], is_reupload: bool = False) -> None:
        if is_reupload:
            self.logger.process_start("Reupload Process", token)
        else:
            self.logger.process_start("File Process", token)

        try:
            with self.queue_lock:
                self.active_tasks += 1
                self.pending_tasks.add(token)

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
                    print(f"Using existing file info for reupload: {file_info.get('filename')}")
                else:
                    print(f"Warning: No database record found for token {token}, fetching from Google Drive")
                    file_info = drive.get_file_info(file_id)

            local_path, filename, filesize = drive.download_file(file_id, token, file_info=file_info, job_type="process_file")

            detected_type = (file_info or {}).get("filetype") or _detect_filetype(filename)
            if detected_type:
                update_method = self.db.reupload_link if is_reupload else self.db.update_link
                try:
                    update_method(token, filetype=detected_type)
                except TypeError:
                    pass
                except Exception:
                    pass
                try:
                    mysql_status_client.update_instant_fields(token=token, filetype=detected_type)
                except TypeError:
                    pass
                except Exception:
                    pass

            enable_gphotos = os.getenv("ENABLE_GPHOTOS", "true").lower() == "true"
            self.logger.info(f"Upload settings - Google Photos: {enable_gphotos}")

            upload_threads: list[threading.Thread] = []
            upload_results: dict[str, str] = {}
            viking_uploader = None

            regular_uploaders = []
            for uploader in get_enabled_uploaders():
                if uploader.name == "VIKINGFILE":
                    viking_uploader = uploader
                else:
                    regular_uploaders.append(uploader)

            for uploader in regular_uploaders:
                t = threading.Thread(target=self._make_upload_runner(uploader, local_path, token, is_reupload, upload_results))
                t.daemon = True
                t.start()
                upload_threads.append(t)

            if enable_gphotos:
                t_gp = threading.Thread(target=self._upload_gphotos, args=(local_path, token, is_reupload, upload_results))
                t_gp.daemon = True
                t_gp.start()
                upload_threads.append(t_gp)
            else:
                print("Google Photos upload disabled by environment variable")

            for t in upload_threads:
                t.join(timeout=300)

            if viking_uploader:
                self.logger.info("🔄 VikingFile: Starting remote upload (Google Photos priority, Worker fallback)")
                t_vk = threading.Thread(target=self._make_upload_runner(viking_uploader, local_path, token, is_reupload, upload_results))
                t_vk.daemon = True
                t_vk.start()
                t_vk.join(timeout=300)

            link_doc = self.db.get_link(token) or {}

            required_fields = []
            if enable_gphotos:
                required_fields.append("gphotos_id")

            for uploader in regular_uploaders:
                if uploader.id_field == "pixeldrain_id":
                    required_fields.append("pixeldrain_id")
                elif uploader.id_field == "buzzheavier_id":
                    required_fields.append("buzzheavier_id")

            if not required_fields:
                self.logger.warning("No upload services enabled, marking as completed")
                has_provider_success = True
            else:
                has_provider_success = any(link_doc.get(field) for field in required_fields)

            if not has_provider_success:
                enabled_services = ", ".join(required_fields)
                raise Exception(f"All enabled upload services failed ({enabled_services})")

            if is_reupload:
                self.logger.process_complete("Reupload Process", token)
                self.db.reupload_link(token, status="completed", error=None)
            else:
                self.logger.process_complete("File Process", token)
                self.db.update_link(token, status="completed", error=None)

            self._update_status_success(token, file_id)

        except Exception as error:
            self._handle_processing_error(token, file_id, error, is_reupload)
        finally:
            self.pending_tasks.discard(token)
            self._schedule_next_task()
            self._cleanup_download(token)

    def _make_upload_runner(self, uploader, local_path, token, is_reupload, upload_results):
        def runner() -> None:
            try:
                if uploader.name == "VIKINGFILE":
                    link_data = self.db.get_link(token) or {}
                    gphotos_id = link_data.get("gphotos_id")
                    drive_file_id = link_data.get("drive_id")
                    filename = link_data.get("filename") or os.path.basename(local_path)

                    self.logger.upload_start("VikingFile", filename)

                    file_id_value = uploader.upload(
                        local_path,
                        token=token,
                        gphotos_id=gphotos_id,
                        file_id=drive_file_id,
                        filename=filename,
                    )
                    db_field = "viking_id"
                else:
                    file_id_value = uploader.upload(local_path, token)
                    db_field = getattr(uploader, "id_field", "") or ""

                if not db_field:
                    db_field = f"{uploader.name.lower()}_id"

                upload_results[db_field] = file_id_value

                update_method = self.db.reupload_link if is_reupload else self.db.update_link
                try:
                    update_method(token, **{db_field: file_id_value})
                except TypeError:
                    pass
                except Exception:
                    pass

                try:
                    mysql_status_client.update_instant_fields(token=token, **{db_field: file_id_value})
                except TypeError:
                    pass
                except Exception:
                    pass

                if uploader.name == "VIKINGFILE":
                    self.logger.upload_success("VikingFile", file_id_value)
                else:
                    self.logger.upload_success(uploader.name, file_id_value)

            except Exception as exc:  # pragma: no cover
                if uploader.name == "VIKINGFILE":
                    self.logger.upload_failed("VikingFile (optional)", str(exc))
                else:
                    self.logger.upload_failed(uploader.name, str(exc))
                    upload_results[f"{uploader.name.lower()}_error"] = str(exc)

        return runner

    def _upload_gphotos(self, local_path: str, token: str, is_reupload: bool, upload_results: Dict[str, str]) -> None:
        try:
            gphotos = GPhotos()
            gphotos_id = gphotos.upload(local_path, token)
            if not gphotos_id:
                return

            encrypted_id = simple_encrypt(gphotos_id)

            gp_identity = os.getenv("GP_IDENTITY", "")
            encrypted_gp_id = simple_encrypt(gp_identity) if gp_identity else None

            update_method = self.db.reupload_link if is_reupload else self.db.update_link
            try:
                update_method(token, gphotos_id=encrypted_id, gp_id=encrypted_gp_id)
            except TypeError:
                try:
                    update_method(token, gphotos_id=encrypted_id)
                except Exception:
                    pass

            try:
                mysql_status_client.update_instant_fields(
                    token=token,
                    media_key=encrypted_id,
                    gphotos_id=encrypted_id,
                    gp_id=encrypted_gp_id,
                )
            except TypeError:
                try:
                    mysql_status_client.update_instant_fields(token=token, media_key=encrypted_id, gphotos_id=encrypted_id)
                except Exception:
                    pass
            except Exception as exc:
                print(f"MySQL instant fields update failed: {str(exc)}")

            upload_results["gphotos_id"] = encrypted_id
            if encrypted_gp_id:
                upload_results["gp_id"] = encrypted_gp_id

            self.logger.upload_success("Google Photos", gphotos_id)

        except Exception as exc:  # pragma: no cover
            self.logger.upload_failed("Google Photos", str(exc))
            upload_results["gphotos_error"] = str(exc)

    def _update_status_success(self, token: str, file_id: str) -> None:
        try:
            print(f"Trying MySQL status update (success) for token {token}")
            updated_via_mysql = mysql_status_client.update_status(
                token=token,
                status="success",
                message="Working",
                drive_url=file_id,
            )
            if updated_via_mysql:
                print(f"MySQL status updated for token {token}")
                return
        except Exception as exc:
            print(f"MySQL status update failed: {str(exc)}")

        api_url = os.getenv("STATUS_API_URL")
        api_key = os.getenv("STATUS_API_KEY")
        if not api_url or not api_key:
            return

        try:
            headers = {"Content-Type": "application/json", "X-API-Key": api_key}
            payload = {"token": token, "status": "success", "message": "Working", "drive_url": file_id}
            print(f"Posting to Status API (success) for token {token}")
            response = requests.post(api_url, json=payload, headers=headers, verify=False)
            response.raise_for_status()
            print(f"Status API updated for token {token}")
        except Exception as exc:  # pragma: no cover
            print(f"Error updating status API: {str(exc)}")

    def _handle_processing_error(self, token: str, file_id: str, error: Exception, is_reupload: bool) -> None:
        error_message = str(error)
        if is_reupload:
            self.logger.error(f"Reupload failed for token {token}: {error_message}")
            if "File not found or no access" in error_message:
                error_message = "File no longer exists in Google Drive or access has been revoked"
            elif "Error checking file size" in error_message:
                error_message = "Unable to access file in Google Drive for reupload"
            self.db.reupload_link(token, status="failed", error=error_message)
        else:
            self.logger.error(f"File processing failed for token {token}: {error_message}")
            self.db.update_link(token, status="failed", error=error_message)

        self._update_status_error(token, file_id, error_message)

    def _update_status_error(self, token: str, file_id: str, error_message: str) -> None:
        try:
            print(f"Trying MySQL status update (error) for token {token}")
            updated_via_mysql = mysql_status_client.update_status(
                token=token,
                status="error",
                message=error_message,
                drive_url=file_id,
            )
            if updated_via_mysql:
                print(f"MySQL status updated for token {token} (error)")
                return
        except Exception as exc:
            print(f"MySQL status update failed (error): {str(exc)}")

        api_url = os.getenv("STATUS_API_URL")
        api_key = os.getenv("STATUS_API_KEY")
        if not api_url or not api_key:
            return

        try:
            headers = {"Content-Type": "application/json", "X-API-Key": api_key}
            payload = {"token": token, "status": "error", "message": error_message, "drive_url": file_id}
            print(f"Posting to Status API (error) for token {token}")
            response = requests.post(api_url, json=payload, headers=headers, verify=False)
            response.raise_for_status()
            print(f"Status API updated for token {token} (error)")
        except Exception as exc:  # pragma: no cover
            print(f"Error updating status API: {str(exc)}")

    def _schedule_next_task(self) -> None:
        with self.queue_lock:
            self.active_tasks -= 1
            if self.task_queue.empty() or self.active_tasks < 0:
                self.active_tasks = max(0, self.active_tasks)
                return

            next_task = self.task_queue.get()
            thread = threading.Thread(target=self.process_file, args=next_task)
            thread.daemon = True
            thread.start()

    def _cleanup_download(self, token: str) -> None:
        download_dir = os.path.join(os.getcwd(), "downloads", token)
        if os.path.exists(download_dir):
            try:
                shutil.rmtree(download_dir)
            except Exception:  # pragma: no cover
                pass

    def enqueue_local_fallback(self, task: TaskArgs) -> None:
        with self.queue_lock:
            if self.active_tasks <= 0:
                thread = threading.Thread(target=self.process_file, args=task)
                thread.daemon = True
                thread.start()
            else:
                self.task_queue.put(task)
