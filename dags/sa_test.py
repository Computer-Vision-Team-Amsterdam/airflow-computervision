import os
from datetime import datetime

from azure.storage.blob import BlobServiceClient
from azure.identity import ManagedIdentityCredential


client_id = os.getenv("USER_ASSIGNED_MANAGED_IDENTITY")
credential = ManagedIdentityCredential(client_id=client_id)


blob_service_client = BlobServiceClient(account_url="https://cvtdataweuogidgmnhwma3zq.blob.core.windows.net", credential=credential)


today = datetime.today().strftime('%Y-%m-%d')
retrieved_images_folder_path = "retrieved_images"


for file in os.listdir(retrieved_images_folder_path):
    blob_client = blob_service_client.get_blob_client(
        container="unblurred", blob=f"{today}/{file}")

    # Upload the created file
    with open(file, "rb") as data:
        blob_client.upload_blob(data)

