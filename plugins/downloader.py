import requests
import os
import tempfile
from urllib.parse import urlparse
import humanize

class Downloader:
    def __init__(self):
        # Create downloads directory in project root
        self.download_dir = os.path.join(os.getcwd(), 'downloads')
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)

    def download(self, url, token):
        # Create token specific directory
        token_dir = os.path.join(self.download_dir, token)
        if not os.path.exists(token_dir):
            os.makedirs(token_dir)
        
        # Get filename from URL or generate one
        filename = os.path.basename(urlparse(url).path)
        if not filename:
            filename = 'downloaded_file'
        
        local_path = os.path.join(token_dir, filename)
        
        # Download with progress
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Get file size
        file_size = int(response.headers.get('content-length', 0))
        human_size = humanize.naturalsize(file_size)
        
        # Download file
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        return local_path, filename, human_size

    def cleanup(self, token):
        # Remove token directory and its contents
        token_dir = os.path.join(self.download_dir, token)
        if os.path.exists(token_dir):
            for file in os.listdir(token_dir):
                os.remove(os.path.join(token_dir, file))
            os.rmdir(token_dir) 