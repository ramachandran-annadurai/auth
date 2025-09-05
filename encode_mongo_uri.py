#!/usr/bin/env python3
"""
MongoDB URI Encoder Utility

This script helps encode MongoDB URIs that contain special characters
in usernames or passwords, which is required for RFC 3986 compliance.
"""

import sys
from urllib.parse import quote_plus

def encode_mongo_uri(uri: str) -> str:
    """
    Properly encode MongoDB URI to handle special characters in username/password.
    """
    try:
        # If the URI already contains encoded characters, return as is
        if '%' in uri and ('@' not in uri or uri.count('@') == 1):
            return uri
        
        # Parse the URI to extract components
        if '://' not in uri:
            return uri
            
        protocol, rest = uri.split('://', 1)
        
        # Check if there's authentication info
        if '@' in rest:
            auth_part, host_part = rest.split('@', 1)
            
            # Check if auth part contains username:password
            if ':' in auth_part:
                username, password = auth_part.split(':', 1)
                # URL encode the username and password
                encoded_username = quote_plus(username)
                encoded_password = quote_plus(password)
                encoded_auth = f"{encoded_username}:{encoded_password}"
            else:
                # Only username provided
                encoded_auth = quote_plus(auth_part)
            
            return f"{protocol}://{encoded_auth}@{host_part}"
        else:
            # No authentication, return as is
            return uri
            
    except Exception as e:
        print(f"Error encoding URI: {e}")
        return uri

def main():
    if len(sys.argv) != 2:
        print("Usage: python encode_mongo_uri.py <mongodb_uri>")
        print("\nExample:")
        print("python encode_mongo_uri.py 'mongodb+srv://user:pass@word@cluster.mongodb.net/dbname'")
        sys.exit(1)
    
    original_uri = sys.argv[1]
    encoded_uri = encode_mongo_uri(original_uri)
    
    print("MongoDB URI Encoder")
    print("=" * 50)
    print(f"Original URI: {original_uri}")
    print(f"Encoded URI:  {encoded_uri}")
    
    if original_uri != encoded_uri:
        print("\n✅ URI has been encoded to handle special characters")
    else:
        print("\n✅ URI doesn't need encoding")

if __name__ == "__main__":
    main()
