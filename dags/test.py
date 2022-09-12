import json
import os
import socket
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient

for k, v in os.environ.items():
    print(f'{k}={v}')

# write to the file checked by Airflow for XComs
f = open('./airflow/xcom/return.json', 'w')
f.write("test-xcom-push")

client_id = os.getenv("AZURE_CLIENT_ID")
credential = DefaultAzureCredential(anaged_identity_client_id=client_id)


airflow_secrets = json.loads(os.environ["AIRFLOW__SECRETS__BACKEND_KWARGS"])
KVUri = airflow_secrets["vault_url"]
print(f"KVURI is {KVUri}")

client = SecretClient(vault_url="https://kv-cvision2-ont-weu-01.vault.azure.net", credential=credential)

client.set_secret("test", "secret")
retrieved_secret = client.get_secret("test")
print(f"Secret is {retrieved_secret}")

"""
password_secret = client.get_secret(name="CloudVpsRawPassword")

socket.setdefaulttimeout(100)
BASE_URL = f"https://3206eec333a04cc980799f75a593505a.objectstore.eu/intermediate/"
USERNAME = username_secret.value
PASSWORD = password_secret.value
"""
