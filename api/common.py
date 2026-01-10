import os
import json
import base64
import hashlib


def load_api_keys() -> dict:
    """Load API keys from local JSON file."""
    try:
        keys_file = os.path.join(os.path.dirname(__file__), 'keys.json')
        with open(keys_file, 'r') as f:
            data = json.load(f)
        return {user['api_key']: user for user in data.get('users', [])}
    except Exception:
        return {}


def validate_api_key(api_key: str) -> bool:
    """Validate API key against keys.json and ensure it's active."""
    api_keys = load_api_keys()
    user = api_keys.get(api_key)
    if not user:
        return False
    return bool(user.get('is_active', False))


def simple_encrypt(text: str | None) -> str | None:
    """Simple symmetric-style obfuscation compatible with PHP helpers.

    Returns URL-safe base64 of a short MD5-derived prefix + plaintext.
    """
    if not text:
        return None

    secret = os.getenv('ENCRYPTION_SECRET', 'mksbhai')
    hash_value = hashlib.md5((text + secret).encode()).hexdigest()[:8]
    result = hash_value + text
    encoded = base64.b64encode(result.encode()).decode()
    encoded = encoded.replace('+', '-').replace('/', '_').replace('=', '')
    return encoded


def simple_decrypt(encrypted_text: str | None) -> str | None:
    """Decrypt text encrypted with simple_encrypt function."""
    if not encrypted_text:
        return None
    
    try:
        # Restore base64 padding and replace URL-safe chars
        padded = encrypted_text.replace('-', '+').replace('_', '/')
        while len(padded) % 4:
            padded += '='
        
        # Decode base64
        decoded = base64.b64decode(padded.encode()).decode()
        
        # Extract hash and original text
        hash_part = decoded[:8]
        original_text = decoded[8:]
        
        # Verify hash
        secret = os.getenv('ENCRYPTION_SECRET', 'mksbhai')
        expected_hash = hashlib.md5((original_text + secret).encode()).hexdigest()[:8]
        
        if hash_part == expected_hash:
            return original_text
        else:
            return None
            
    except Exception:
        return None


