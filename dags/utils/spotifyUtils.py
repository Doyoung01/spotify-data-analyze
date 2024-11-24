import requests
import base64
from airflow.models import Variable

def send_request(params, url):
    response = requests.get(url, headers=get_headers(), params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"status code: {response.status_code}")
        print(response.json())
        raise

def get_headers():
    client_id = Variable.get("CLIENT_ID")
    client_secret = Variable.get("CLIENT_SECRET")

    # Spotify Access Token URL
    token_url = "https://accounts.spotify.com/api/token"
    
    # Base64 인코딩된 Client ID와 Secret
    auth_string = f"{client_id}:{client_secret}"
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = base64.b64encode(auth_bytes).decode("utf-8")
    
    headers = {
        "Authorization": f"Basic {auth_base64}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "client_credentials"
    }
    
    # POST 요청으로 Access Token 가져오기
    response = requests.post(token_url, headers=headers, data=data)
    
    if response.status_code == 200:
        access_token = response.json().get("access_token")
        return {
            "Authorization": f"Bearer {access_token}"
        }
    else:
        print(f"Failed to retrieve token: {response.status_code}")
        print(response.json())
        raise