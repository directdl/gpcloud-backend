from flask import Blueprint, jsonify
import json
import os

# Create blueprint
token_api = Blueprint('token_api', __name__)

from api.common import validate_api_key
from plugins.database import Database
db = Database()

# Deprecated local key loaders removed; using api.common.validate_api_key

@token_api.route('/api/<api_key>/token/<token>', methods=['GET'])
def get_token_info(api_key, token):
    """Get basic information for a token"""
    try:
        # Validate API key
        if not validate_api_key(api_key):
            return jsonify({
                'success': False,
                'message': 'Invalid or inactive API key'
            }), 401
        
        # Get link data directly from MongoDB
        link_data = db.links.find_one({'token': token})
        
        if not link_data:
            return jsonify({
                'success': False,
                'message': 'Token not found'
            }), 404
            
        # Return only the required fields
        return jsonify({
            'success': True,
            'data': {
                'token': link_data['token'],
                'drive_id': link_data['drive_id'],
                'filename': link_data.get('filename', ''),
                'filesize': link_data.get('filesize', ''),
                'pixeldrain_id': link_data.get('pixeldrain_id', ''),
                'buzzheavier_id': link_data.get('buzzheavier_id', ''),
                'gphotos_id': link_data.get('gphotos_id', '')
            }
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500
