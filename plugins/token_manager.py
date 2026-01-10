import pickle
import os
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

class TokenManager:
    def __init__(self):
        self.token_file = os.path.join("private", "token.pickle")
        self.scopes = ["https://www.googleapis.com/auth/drive"]
        self.credentials_content = None
        
    def get_auth_url(self, credentials_file: str):
        """Get authorization URL"""
        try:
            # Read and store credentials content
            with open(credentials_file, 'r') as f:
                self.credentials_content = f.read()
                
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_file,
                self.scopes
            )
            
            # Set redirect URI
            flow.redirect_uri = "http://127.0.0.1:53682"
            
            auth_url = flow.authorization_url()[0]
            return auth_url
            
        except Exception as e:
            raise
        
    async def generate_token(self, code: str) -> bool:
        """Generate token from auth code"""
        try:
            if not self.credentials_content:
                return False
                
            # Write credentials content to temp file
            temp_cred_file = os.path.join("private", "temp_cred.json")
            with open(temp_cred_file, 'w') as f:
                f.write(self.credentials_content)
                
            flow = InstalledAppFlow.from_client_secrets_file(
                temp_cred_file,
                self.scopes
            )
            
            # Set redirect URI
            flow.redirect_uri = "http://127.0.0.1:53682"
            
            # Get credentials
            flow.fetch_token(code=code)
            credentials = flow.credentials
            
            # Create private directory if it doesn't exist
            os.makedirs("private", exist_ok=True)
            
            # Save the credentials
            with open(self.token_file, 'wb') as token:
                pickle.dump(credentials, token)
                
            # Cleanup
            os.remove(temp_cred_file)
            
            return True
            
        except Exception:
            if os.path.exists(temp_cred_file):
                os.remove(temp_cred_file)
            return False
            
    def load_token(self):
        """Load token from pickle file"""
        try:
            if os.path.exists(self.token_file):
                with open(self.token_file, 'rb') as token:
                    credentials = pickle.load(token)
                    
                if credentials and credentials.valid:
                    return credentials.token
                    
                if credentials and credentials.expired and credentials.refresh_token:
                    credentials.refresh(Request())
                    with open(self.token_file, 'wb') as token:
                        pickle.dump(credentials, token)
                    return credentials.token
                    
            return None
            
        except Exception:
            return None 