import os

from azure.storage.blob import BlobServiceClient
from azure.identity import ManagedIdentityCredential


client_id = os.getenv("USER_ASSIGNED_MANAGED_IDENTITY")
credential = ManagedIdentityCredential(client_id=client_id)

#airflow_secrets = json.loads(os.environ["AIRFLOW__SECRETS__BACKEND_KWARGS"])
#KVUri = airflow_secrets["vault_url"]

creds = ManagedIdentityCredential(client_id)

blob_service_client = BlobServiceClient(account_url="https://cvtdataweuogidgmnhwma3zq.blob.core.windows.net", credential=creds)
test = blob_service_client.list_containers()
for container in test:
    print(container.name)
