"""
CLI that uses Typer to build the command line interface.

python cli.py --help

python cli.py all
python cli.py mssql
python cli.py postgres
python cli.py mongodb
python cli.py redis

python cli.py rides
"""

import typer
import os

from rich import print
from main import BlobStorage
from dotenv import load_dotenv

load_dotenv()

blob_storage_conn_str = os.getenv("BLOB_STORAGE_CONNECTION_STRING")
container_landing = os.getenv("LANDING_CONTAINER_NAME")


def main(dstype: str):
    """
    Perform actions based on the specified data source type.

    Allowed types are: mssql, postgres, mongodb, redis, all

    Args:
        dstype: The type of the data source.
    """

    if dstype == "mssql":
        print(BlobStorage(blob_storage_conn_str, container_landing).write_file(ds_type="mssql"))
    elif dstype == "postgres":
        print(BlobStorage(blob_storage_conn_str, container_landing).write_file(ds_type="postgres"))
    elif dstype == "mongodb":
        print(BlobStorage(blob_storage_conn_str, container_landing).write_file(ds_type="mongodb"))
    elif dstype == "redis":
        print(BlobStorage(blob_storage_conn_str, container_landing).write_file(ds_type="redis"))
    elif dstype == "rides":
        print(BlobStorage(blob_storage_conn_str, container_landing).write_file(ds_type="rides"))
    elif dstype == "all":
        print(BlobStorage(blob_storage_conn_str, container_landing).write_file(ds_type="mssql"))
        print(BlobStorage(blob_storage_conn_str, container_landing).write_file(ds_type="postgres"))
        print(BlobStorage(blob_storage_conn_str, container_landing).write_file(ds_type="mongodb"))
        # TODO print(BlobStorage(blob_storage_conn_str, container_landing).write_file(ds_type="redis"))


if __name__ == "__main__":
    typer.run(main)
