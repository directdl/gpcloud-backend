import requests
import os
import base64
import random
import time
from typing import Optional
from requests.adapters import HTTPAdapter, Retry
from dotenv import load_dotenv
from utils.logging_config import get_modern_logger

load_dotenv()
logger = get_modern_logger("PIXELDRAIN")


class PixelDrain:
    def __init__(self):
        self.base_url = os.getenv('PIXELDRAIN_API_URL', 'https://pixeldrain.com/api')
        api_keys_str = os.getenv('PIXELDRAIN_API_KEY', '')
        self.api_keys = [key.strip() for key in api_keys_str.split(',') if key.strip()]
        self.current_key_index = 0
        if not self.api_keys:
            raise ValueError("No PixelDrain API keys found in PIXELDRAIN_API_KEY environment variable")

        # Networking
        self.timeout = int(os.getenv('PIXELDRAIN_TIMEOUT', '60'))
        self.session = requests.Session()
        retries = Retry(
            total=int(os.getenv('PIXELDRAIN_RETRIES', '5')),
            backoff_factor=float(os.getenv('PIXELDRAIN_BACKOFF', '0.5')),
            status_forcelist=[429, 500, 502, 503, 504],
            respect_retry_after_header=True,
            allowed_methods=["GET", "PUT", "DELETE"],
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

        # Pacing
        self.min_sleep = float(os.getenv('PIXELDRAIN_MIN_SLEEP_S', '0.05'))
        self.max_sleep = float(os.getenv('PIXELDRAIN_MAX_SLEEP_S', '2'))

        logger.success(f"PixelDrain: Loaded {len(self.api_keys)} API keys")

    def _get_headers(self, api_key: Optional[str] = None):
        if api_key is None:
            api_key = self.api_keys[self.current_key_index]
        return {'Authorization': f'Basic {base64.b64encode(f":{api_key}".encode()).decode()}'}

    def _switch_to_next_key(self):
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        print(f"PixelDrain: Switched to API key {self.current_key_index + 1}/{len(self.api_keys)}")

    def _respect_retry_after(self, response: Optional[requests.Response], attempt: int):
        # Sleep using Retry-After if provided; else exponential backoff with jitter
        if response is not None:
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                try:
                    delay = float(retry_after)
                    time.sleep(min(self.max_sleep, max(self.min_sleep, delay)))
                    return
                except Exception:
                    pass
        # Exponential backoff with jitter
        sleep_s = min(self.max_sleep, max(self.min_sleep, (2 ** attempt) * 0.1))
        # add small jitter
        sleep_s = sleep_s * (0.8 + 0.4 * random.random())
        time.sleep(sleep_s)

    def upload(self, file_path: str) -> str:
        # Randomize starting key to spread load
        if self.api_keys:
            self.current_key_index = random.randrange(len(self.api_keys))
            print(f"PixelDrain: Random start with API key {self.current_key_index + 1}/{len(self.api_keys)}")

        tried_keys = set()
        attempt = 0
        while len(tried_keys) < len(self.api_keys):
            try:
                current_key = self.api_keys[self.current_key_index]
                if current_key in tried_keys:
                    self._switch_to_next_key()
                    continue
                tried_keys.add(current_key)

                headers = self._get_headers()
                print(f"PixelDrain: Attempting upload with API key {self.current_key_index + 1}/{len(self.api_keys)}")

                with open(file_path, 'rb') as f:
                    filename = os.path.basename(file_path)
                    resp = self.session.put(
                        f"{self.base_url}/file/{filename}",
                        headers=headers,
                        data=f,
                        timeout=self.timeout,
                    )
                    if resp.status_code in (401, 403, 429):
                        print(f"PixelDrain: Key {self.current_key_index + 1} rejected ({resp.status_code})")
                        self._respect_retry_after(resp, attempt)
                        self._switch_to_next_key()
                        attempt += 1
                        continue
                    resp.raise_for_status()
                    file_id = resp.json()['id']
                    print(f"PixelDrain: Upload successful with API key {self.current_key_index + 1}")
                    return file_id

            except requests.HTTPError as e:
                resp = getattr(e, 'response', None)
                status = resp.status_code if resp is not None else 'NA'
                print(f"PixelDrain: HTTP error ({status}) on key {self.current_key_index + 1}: {e}")
                self._respect_retry_after(resp, attempt)
                self._switch_to_next_key()
                attempt += 1
                continue
            except Exception as e:
                print(f"PixelDrain: Error with API key {self.current_key_index + 1}: {str(e)}")
                self._respect_retry_after(None, attempt)
                self._switch_to_next_key()
                attempt += 1
                continue

        raise Exception(f"All {len(self.api_keys)} PixelDrain API keys failed")

    def get_file_info(self, file_id: str):
        tried_keys = set()
        attempt = 0
        while len(tried_keys) < len(self.api_keys):
            try:
                current_key = self.api_keys[self.current_key_index]
                if current_key in tried_keys:
                    self._switch_to_next_key()
                    continue
                tried_keys.add(current_key)

                headers = self._get_headers()
                resp = self.session.get(
                    f"{self.base_url}/file/{file_id}/info",
                    headers=headers,
                    timeout=self.timeout,
                )
                if resp.status_code in (401, 403, 429):
                    self._respect_retry_after(resp, attempt)
                    self._switch_to_next_key()
                    attempt += 1
                    continue
                resp.raise_for_status()
                return resp.json()

            except requests.HTTPError as e:
                resp = getattr(e, 'response', None)
                self._respect_retry_after(resp, attempt)
                self._switch_to_next_key()
                attempt += 1
                continue
            except Exception:
                self._respect_retry_after(None, attempt)
                self._switch_to_next_key()
                attempt += 1
                continue

        raise Exception(f"All {len(self.api_keys)} PixelDrain API keys failed")

    def delete_file(self, file_id: str):
        tried_keys = set()
        attempt = 0
        while len(tried_keys) < len(self.api_keys):
            try:
                current_key = self.api_keys[self.current_key_index]
                if current_key in tried_keys:
                    self._switch_to_next_key()
                    continue
                tried_keys.add(current_key)

                headers = self._get_headers()
                resp = self.session.delete(
                    f"{self.base_url}/file/{file_id}",
                    headers=headers,
                    timeout=self.timeout,
                )
                if resp.status_code in (401, 403, 429):
                    self._respect_retry_after(resp, attempt)
                    self._switch_to_next_key()
                    attempt += 1
                    continue
                resp.raise_for_status()
                return resp.json()

            except requests.HTTPError as e:
                resp = getattr(e, 'response', None)
                self._respect_retry_after(resp, attempt)
                self._switch_to_next_key()
                attempt += 1
                continue
            except Exception:
                self._respect_retry_after(None, attempt)
                self._switch_to_next_key()
                attempt += 1
                continue

        raise Exception(f"All {len(self.api_keys)} PixelDrain API keys failed")