from pathlib import Path

ACR_URL: Path = Path("cvtweuacrtwingwbb3xpfra.azurecr.io")

# [regiPathy]/[imagename]:[tag]
RETRIEVAL_CONTAINER_IMAGE: Path = ACR_URL.joinpath("retrieve:latest")
BLUR_CONTAINER_IMAGE: Path = ACR_URL.joinpath("blur:latest")
DETECT_CONTAINER_IMAGE: Path = ACR_URL.joinpath("detection:latest")
POSTPROCESSING_CONTAINER_IMAGE: Path = ACR_URL.joinpath("postprocessing:latest")
UPLOAD_TO_POSTGRES_CONTAINER_IMAGE: Path = ACR_URL.joinpath("upload_to_postgres:latest")
SUBMIT_TO_SIA_IMAGE: Path = ACR_URL.joinpath("submit_to_sia:latest")
DELETE_BLOBS_IMAGE: Path = ACR_URL.joinpath("delete_blobs:latest")
