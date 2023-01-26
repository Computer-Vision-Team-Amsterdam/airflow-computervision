import importlib

from common import OTAP_ENVIRONMENT


# Variables that should be present in all environments
__all__ = [
    "BLOB_URL",
    "BLUR_CONTAINER_IMAGE",
    "DELETE_BLOBS_IMAGE",
    "DETECT_CONTAINER_IMAGE",
    "POSTPROCESSING_CONTAINER_IMAGE",
    "RETRIEVAL_CONTAINER_IMAGE",
    "SUBMIT_TO_SIA_IMAGE",
    "UPLOAD_TO_POSTGRES_CONTAINER_IMAGE",
    "RETRIEVAL_CONTAINER_IMAGE_OLD"
]

module_name = ""
if OTAP_ENVIRONMENT.lower().endswith("ont"):
    module_name = "environment.development"
if OTAP_ENVIRONMENT.lower().endswith("tst"):
    module_name = "environment.test"
if OTAP_ENVIRONMENT.lower().endswith("acc"):
    module_name = "environment.acceptance"
if OTAP_ENVIRONMENT.lower().endswith("prd"):
    module_name = "environment.production"

module = importlib.import_module(module_name)
globals().update({x: getattr(module, x) for x in __all__})
