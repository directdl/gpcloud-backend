import requests
import os
import base64
from dotenv import load_dotenv
from config import BUZZHEAVIER_ACCOUNT_ID

load_dotenv()

class BuzzHeavier:
    def __init__(self):
        self.base_url = "https://w.fuckingfast.net"
        self.api_key = BUZZHEAVIER_ACCOUNT_ID
        self.headers = {
            'Authorization': f'Bearer {self.api_key}'
        }
        # This class should not touch database directly
        
    def upload(self, file_path, token=None, note=None):
        """
        Upload a file to BuzzHeavier using their API and return the file ID.
        Database updates are handled by callers.
        """
        try:
            filename = os.path.basename(file_path)
            url = f"{self.base_url}/{filename}"
            
            # Add note if provided
            if note:
                note_b64 = base64.b64encode(note.encode()).decode()
                url = f"{url}?note={note_b64}"
            
            with open(file_path, 'rb') as f:
                response = requests.put(
                    url,
                    headers=self.headers if self.api_key else {},
                    data=f
                )
                response.raise_for_status()
                
                # Get ID from response
                response_data = response.json()
                file_id = response_data['data']['id']
                return file_id
                
        except Exception as e:
            # Let caller handle database and error propagation
            raise
            
    def upload_to_directory(self, file_path, parent_id, token=None):
        """
        Upload a file to a specific directory in BuzzHeavier and return the file ID.
        Database updates are handled by callers.
        """
        try:
            filename = os.path.basename(file_path)
            url = f"{self.base_url}/{parent_id}/{filename}"
            
            with open(file_path, 'rb') as f:
                response = requests.put(
                    url,
                    headers=self.headers,
                    data=f
                )
                response.raise_for_status()
                
                # Get ID from response
                response_data = response.json()
                file_id = response_data['data']['id']
                return file_id
                
        except Exception as e:
            # Let caller handle database and error propagation
            raise