import json
import os
import socket

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

for k, v in os.environ.items():
    print(f'{k}={v}')

credential = DefaultAzureCredential()
# KVUri = f"https://kv-cvision2-ont-weu-01.vault.azure.net"

airflow_secrets = json.loads(os.environ["AIRFLOW__SECRETS__BACKEND_KWARGS"])
KVUri = airflow_secrets["vault_url"]
print(f"KVURI is {KVUri}")

client = SecretClient(vault_url=KVUri, credential=credential)

username_secret = client.get_secret("CloudVpsRawUsername")
try:
    print(f"username is {json.loads(username_secret.value)}")
except:
    print("first way of printing is wrong")

try:
    print(f"username is {username_secret.value}")
except:
    print("second way of printing is wrong")
"""
password_secret = client.get_secret(name="CloudVpsRawPassword")

socket.setdefaulttimeout(100)
BASE_URL = f"https://3206eec333a04cc980799f75a593505a.objectstore.eu/intermediate/"
USERNAME = username_secret.value
PASSWORD = password_secret.value
"""
