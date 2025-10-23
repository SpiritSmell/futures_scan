import requests
import os

# Path to the system's CA bundle
SYSTEM_CA_BUNDLE = '/etc/ssl/certs/ca-certificates.crt'

# Set the CA bundle explicitly for requests
os.environ['REQUESTS_CA_BUNDLE'] = SYSTEM_CA_BUNDLE

url = 'https://api.hbdm.com/api/v1/timestamp'

try:
    print(f"Using CA bundle at: {SYSTEM_CA_BUNDLE}")
    response = requests.get(url, timeout=10)
    response.raise_for_status()  # Raise an exception for bad status codes
    print("Successfully connected to the htx API.")
    print("Response:", response.json())
except requests.exceptions.SSLError as e:
    print(f"SSL Error occurred: {e}")
except requests.exceptions.RequestException as e:
    print(f"A request error occurred: {e}")
