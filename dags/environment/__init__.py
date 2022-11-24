import importlib

from common import OTAP_ENVIRONMENT


# Variables that should be present in all environments
__all__ = [
    "BLUR_CONTAINER_IMAGE",
    "DELETE_BLOBS_IMAGE",
    "DETECT_CONTAINER_IMAGE",
    "POSTPROCESSING_CONTAINER_IMAGE",
    "RETRIEVAL_CONTAINER_IMAGE",
    "SUBMIT_TO_SIA_IMAGE",
    "UPLOAD_TO_POSTGRES_CONTAINER_IMAGE",
]


module_name = ""
if OTAP_ENVIRONMENT == "ont":
    module_name = "dags.environment.development"
if OTAP_ENVIRONMENT == "tst":
    module_name = "dags.environment.test"
if OTAP_ENVIRONMENT == "acc":
    module_name = "dags.environment.acceptance"
if OTAP_ENVIRONMENT == "prd":
    module_name = "dags.environment.production"

module = importlib.import_module(module_name)
globals().update({x: getattr(module, x) for x in __all__})
