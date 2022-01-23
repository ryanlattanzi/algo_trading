from datetime import datetime
import os
from io import StringIO

import yaml as yl
import pandas as pd
from algo_trading.logger.controllers import LogConfig, LogLevelController

from algo_trading.repositories.obj_store_repository import ObjStoreRepository
from algo_trading.utils.utils import dt_to_str

"""
Assuming we are running the test from our local machine, we set the
endpoint to localhost:9000. If we are to Dockerize the app, we need to change it
accordingly to the network location.
"""

# Loading in and parsing config
# NEED TO CHANGE THIS TO ENVIRONMENT VARIABLES INSTEAD
config_path = os.path.join(
    "/Users/ryanlattanzi/Desktop/projects/trading_app/algo_trading",
    "config/config.yml",
)
config = yl.safe_load(open(config_path, "r"))
obj_store_handler = config["obj_store_repo"]

obj_store_info = {
    "endpoint_url": "http://localhost:9000",
    "aws_access_key_id": "admin",
    "aws_secret_access_key": "password",
    "region_name": "us-east-1",
}

log_info = LogConfig(
    log_name="obj_store_repository_test",
    file_name=os.path.join(
        "logs", f"obj_store_repository_test_{dt_to_str(datetime.today())}.log"
    ),
    log_level=LogLevelController.info,
)

obj_store_repo = ObjStoreRepository(
    obj_store_info=obj_store_info,
    obj_handler=obj_store_handler,
    log_info=log_info,
)

obj_store_handler = obj_store_repo.handler


def test_bucket_integration():

    # Creating a Bucket
    bucket_name = "test-bucket"
    obj_store_handler.create_bucket(bucket_name)
    buckets = obj_store_handler.list_buckets()
    # print(buckets["Buckets"])
    assert bucket_name in [bucket["Name"] for bucket in buckets["Buckets"]]
    print("Created bucket.")

    # Uploading a File
    data = pd.read_csv("sample_data.csv")
    obj_key = "data/sample/{ticker}/{date}.csv"
    file_name = obj_key.format(ticker="testing", date=dt_to_str(datetime.today()))
    csv_buffer = StringIO()
    data.to_csv(csv_buffer)
    obj_store_handler.put_object(
        file_body=csv_buffer.getvalue(),
        bucket=bucket_name,
        key=file_name,
    )
    files = obj_store_handler.list_objects(bucket_name)
    # print(files["Contents"])
    assert file_name in [file["Key"] for file in files["Contents"]]
    print("Uploaded file.")

    # Deleting the File
    obj_store_handler.delete_objects(bucket=bucket_name, objects=[file_name])
    files = obj_store_handler.list_objects(bucket_name)
    # print(files)
    assert "Contents" not in files
    print("Deleted file.")

    # Deleting the bucket
    obj_store_handler.delete_bucket(bucket_name)
    buckets = obj_store_handler.list_buckets()
    assert bucket_name not in [bucket["Name"] for bucket in buckets["Buckets"]]
    print("Deleted bucket.")


if __name__ == "__main__":
    test_bucket_integration()
