import requests
from datetime import datetime

class AuthManager:
    def __init__(self, auth_url, client_id, client_secret):
        self.auth_url = auth_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.token_expiry = None

    def get_bearer_token(self):
        auth_payload = {
            "clientId": self.client_id,
            "clientSecret": self.client_secret
        }

        auth_headers = {
            'Content-Type': 'application/json'
        }

        try:
            # Send the request to the authentication URL
            auth_response = requests.post(self.auth_url, headers=auth_headers, json=auth_payload)
            auth_response.raise_for_status() 
            auth_res = auth_response.json()

            access_token = auth_res['accessToken']
            expires_on = int(auth_res['expiresOn'])
            self.token_expiry = datetime.fromtimestamp(expires_on)

            self.token = access_token
            print(f"Token received: {access_token}, expires on: {self.token_expiry}")
            return access_token, expires_on

        except requests.exceptions.RequestException as e:
            print(f"Error fetching token: {e}")
            return None, None

    def is_token_expired(self):
        return self.token is None or datetime.now() >= self.token_expiry