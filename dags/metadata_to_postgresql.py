# This module retrieves the metadata of the raw images, queries the panorama API and stores the metadata in the
# metadata table

# TODO put postgres secrets into DaVe KV
# TODO update global variables below
# TODO open postgresql to see what kind of structure you need + name of the table
# TODO write simple test to see which metadata there is, here's an example id: TMX7315120208-000020_pano_0000_000000
# TODO find function to upload dicts/lists to postgres and replace current cvs

# general
import os
import json
import socket
import argparse

from typing import List, Any

import psycopg2
from psycopg2 import Error

from panorama.client import PanoramaClient
from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient


# ============================ CONNECTIONS =========================== #
client_id = os.getenv("USER_ASSIGNED_MANAGED_IDENTITY")
credential = ManagedIdentityCredential(client_id=client_id)

airflow_secrets = json.loads(os.environ["AIRFLOW__SECRETS__BACKEND_KWARGS"])
KVUri = airflow_secrets["vault_url"]

client = SecretClient(vault_url=KVUri, credential=credential)
username_secret = client.get_secret(name="TODO")
password_secret = client.get_secret(name="TODO")
host_secret = client.get_secret(name="TODO")
socket.setdefaulttimeout(100)

USERNAME = username_secret.value
PASSWORD = password_secret.value
HOST = host_secret.value
PORT = "5432"
DATABASE = "TODO"

# ============================ CONNECTIONS =========================== #


def get_metadata_from_id(panorama_id: str):
    """

    :param panorama_id:
    :return:
    """
    pano_object = PanoramaClient.get_panorama(panorama_id)
    pano_metadata = pano_object

    return pano_metadata


def save_metadata(panorama_ids: list[str]) -> List[Any]:
    """
    This method stores certain metadata according to the defintion below
    :param panorama_ids:
    :return:
    """

    metadata_complete = list()
    for pano_id in panorama_ids:
        metadata_pano = get_metadata_from_id(pano_id)
        metadata_complete.append(metadata_pano)

    return metadata_complete


def upload_metadata(metadata: Any, date: str):
    """
    This method upload the list of metadata information to the postgres database
    :return:
    """

    connection = None
    try:
        # Connect to an existing database
        connection = psycopg2.connect(
            user=USERNAME,
            password=PASSWORD,
            host=HOST,
            port=PORT,
            database=DATABASE,
        )
        # Create a cursor to perform database operations
        cursor = connection.cursor()
        # Print PostgreSQL details
        print("PostgreSQL server information")

        # Executing a SQL query
        with open("TODO", "r") as f:
            next(f)  # Skip the header row.
            cursor.copy_from(f, "TODO", sep=";")

        connection.commit()

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

# ============= MAIN METHOD ============= #


def metadata_processing():
    """
    Main method to be run in the pipeline dag.
    """
    # parse date argument
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="date to retrieve images")
    opt = parser.parse_args()

    panorama_ids = list()
    metadata = save_metadata(panorama_ids)
    upload_metadata(metadata, date=opt.date)
