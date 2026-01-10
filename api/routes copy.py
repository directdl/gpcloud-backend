from flask import Blueprint, jsonify, request
import secrets
import threading
import os
import shutil
import json
from plugins.database import Database
from plugins.drive import GoogleDrive
from plugins.pixeldrain import PixelDrain
from plugins.buzzheavier import BuzzHeavier
import requests

# Create blueprint
api = Blueprint('api', __name__)

# Initialize database
db = Database()

def load_api_keys():
    """Load API keys from JSON file"""
    try:
        keys_file = os.path.join(os.path.dirname(__file__), 'keys.json')
        with open(keys_file, 'r') as f:
            data = json.load(f)
        return {user['api_key']: user for user in data['users']}
    except Exception:
        return {}

def validate_api_key(api_key):
    """Validate API key"""
    api_keys = load_api_keys()
    user = api_keys.get(api_key)
    
    if not user:
        return False
        
    return user['is_active']

def process_file(token, file_id, file_info):
    """Background process to download and upload file"""
    try:
        drive = GoogleDrive()
        pixeldrain = PixelDrain()
        buzzheavier = BuzzHeavier()
        
        # Download file from Google Drive
        local_path, filename, filesize = drive.download_file(file_id, token)
        
        # Upload to Pixeldrain
        pixeldrain_id = pixeldrain.upload(local_path)
        
        # Upload to BuzzHeavier (without auto upload)
        buzzheavier_id = buzzheavier.upload(local_path, token)
        
        # Update database
        db.update_link(token, 
                      status='completed',
                      pixeldrain_id=pixeldrain_id,
                      buzzheavier_id=buzzheavier_id)
        
        # Update status API
        try:
            api_url = os.getenv('STATUS_API_URL')
            api_key = os.getenv('STATUS_API_KEY')
            
            if api_url and api_key:
                headers = {
                    'Content-Type': 'application/json',
                    'X-API-Key': api_key
                }
                
                data = {
                    'token': token,
                    'status': 'success',
                    'message': 'Working',
                    'drive_url': file_id
                }
                
                response = requests.post(api_url, json=data, headers=headers)
                response.raise_for_status()
                print(f"Status API updated for token {token}")
        except Exception as e:
            print(f"Error updating status API: {str(e)}")
        
    except Exception as e:
        db.update_link(token, 
                      status='failed',
                      error=str(e))
                      
        # Update status API for error
        try:
            api_url = os.getenv('STATUS_API_URL')
            api_key = os.getenv('STATUS_API_KEY')
            
            if api_url and api_key:
                headers = {
                    'Content-Type': 'application/json',
                    'X-API-Key': api_key
                }
                
                data = {
                    'token': token,
                    'status': 'error',
                    'message': str(e),
                    'drive_url': file_id
                }
                
                response = requests.post(api_url, json=data, headers=headers)
                response.raise_for_status()
                print(f"Status API updated for token {token} (error)")
        except Exception as api_error:
            print(f"Error updating status API: {str(api_error)}")
    
    finally:
        # Cleanup downloaded files
        try:
            download_dir = os.path.join(os.getcwd(), 'downloads', token)
            if os.path.exists(download_dir):
                shutil.rmtree(download_dir)
        except Exception:
            pass

@api.route('/<api_key>/<path:drive_path>', methods=['GET'])
def generate_link(api_key, drive_path):
    """Generate download link from Google Drive link or ID"""
    try:
        # Validate API key
        if not validate_api_key(api_key):
            return jsonify({
                'success': False,
                'error': 'Invalid or inactive API key'
            }), 401
        
        # Initialize Google Drive
        drive = GoogleDrive()
        
        # Check if input is a full URL or just an ID
        if 'drive.google.com' in drive_path:
            file_id = drive.extract_file_id(drive_path)
            if not file_id:
                return jsonify({
                    'success': False,
                    'error': 'Invalid Google Drive link'
                }), 400
        else:
            file_id = drive_path
            
        # Get file info first to check size
        try:
            file_info = drive.get_file_info(file_id)
            
            # Check file size limit
            size_str = str(file_info['filesize']).upper()
            if 'GB' in size_str:
                size_gb = float(size_str.replace('GB', ''))
            elif 'MB' in size_str:
                size_gb = float(size_str.replace('MB', '')) / 1024
            elif 'TB' in size_str:
                size_gb = float(size_str.replace('TB', '')) * 1024
            else:
                size_gb = 0
                
            max_size = float(os.getenv('MAX_FILE_SIZE_GB', '6.5'))
            if size_gb > max_size:
                return jsonify({
                    'success': False,
                    'error': f'File size ({file_info["filesize"]}) exceeds maximum limit of {max_size}GB',
                    'max_size': f'{max_size}GB',
                    'file_size': file_info["filesize"]
                }), 400
                
        except Exception as e:
            return jsonify({
                'success': False,
                'error': f'Error checking file size: {str(e)}'
            }), 400

        # Check if this file_id already exists in database
        existing_link = db.find_link_by_drive_id(file_id)
        if existing_link:
            # Return existing link data with 303 See Other status
            response = jsonify({
                'success': True,
                'message': 'Existing link found',
                'data': {
                    'token': existing_link['token'],
                    'filename': existing_link.get('filename'),
                    'filesize': existing_link.get('filesize'),
                    'status': existing_link['status'],
                    'check_url': f"/api/{api_key}/status/{existing_link['token']}"
                }
            })
            response.status_code = 303
            response.headers['Location'] = f"/api/{api_key}/status/{existing_link['token']}"
            return response

        # Generate new token and save link
        token = secrets.token_urlsafe(16)
        db.save_link(token=token,
                    drive_id=file_id,
                    status='pending',
                    filename=file_info['filename'],
                    filesize=file_info['filesize'])
        
        # Start background processing
        thread = threading.Thread(
            target=process_file,
            args=(token, file_id, file_info)
        )
        thread.daemon = True
        thread.start()
        
        # Return response with file info
        return jsonify({
            'success': True,
            'message': 'Link generation started',
            'data': {
                'token': token,
                'filename': file_info['filename'],
                'filesize': file_info['filesize'],
                'status': 'pending',
                'check_url': f"/api/{api_key}/status/{token}"
            }
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@api.route('/<api_key>/status/<token>', methods=['GET'])
def check_status(api_key, token):
    """Check status of a download"""
    try:
        # Validate API key
        if not validate_api_key(api_key):
            return jsonify({
                'success': False,
                'error': 'Invalid or inactive API key'
            }), 401
        
        # Get link data
        link_data = db.get_link(token)
        if not link_data:
            return jsonify({
                'success': False,
                'error': 'Link not found'
            }), 404
        
        response = {
            'success': True,
            'data': {
                'token': token,
                'status': link_data['status'],
                'filename': link_data.get('filename'),
                'filesize': link_data.get('filesize')
            }
        }
        
        # Add download URLs if completed
        if link_data['status'] == 'completed':
            response['data']['pixeldrain_url'] = f"https://pixeldrain.com/u/{link_data['pixeldrain_id']}"
            if link_data.get('buzzheavier_id'):
                response['data']['buzzheavier_url'] = f"https://buzzheavier.com/{link_data['buzzheavier_id']}"
        # Add error if failed
        elif link_data['status'] == 'failed':
            response['data']['error'] = link_data.get('error')
            
        return jsonify(response)
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500 