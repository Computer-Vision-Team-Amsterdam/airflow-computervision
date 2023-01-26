from pathlib import Path

ACR_URL ="cvtweuacrogidgmnhwma3zq.azurecr.io"
BLOB_URL = "https://cvtdataweuogidgmnhwma3zq.blob.core.windows.net"

# [regiPathy]/[imagename]:[tag]
BLUR_CONTAINER_IMAGE = ACR_URL + "/blur:latest"
DELETE_BLOBS_IMAGE = ACR_URL + "/delete_blobs:latest"
DETECT_CONTAINER_IMAGE = ACR_URL + "/detection:latest"
POSTPROCESSING_CONTAINER_IMAGE = ACR_URL + "/postprocessing:latest"
RETRIEVAL_CONTAINER_IMAGE = ACR_URL + "/retrieve_images:latest"
RETRIEVAL_CONTAINER_IMAGE_OLD = ACR_URL + "/retrieve:latest"
SUBMIT_TO_SIA_IMAGE = ACR_URL + "/submit_to_sia:latest"
UPLOAD_TO_POSTGRES_CONTAINER_IMAGE = ACR_URL + "/upload_to_postgres:latest"
