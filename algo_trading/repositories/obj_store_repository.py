from logging import Logger
from typing import Dict, List
import boto3
from abc import ABC, abstractmethod, abstractproperty
from pydantic import validate_arguments

from algo_trading.config import OBJ_STORE_INFO
from algo_trading.config.controllers import ObjStoreController
from algo_trading.logger.controllers import LogConfig
from algo_trading.logger.default_logger import child_logger


class AbstractObjStore(ABC):
    """Abstract repository to define common methods that
    persist raw data in an object storage location before
    loading into the DB. Currently, Amazon S3 and MinIO
    are supported.

    Args:
        ABC ([type]): Abstract Base Class
    """

    @abstractproperty
    def client(self) -> boto3.client:
        """Client to interact with simple storage services.
        Currently supports Amazon S3 and MinIO, both of which
        use the boto3 Client.

        The client is constructed given the kwargs in __init__.

        Returns:
            boto3.client: Object storage client.
        """
        pass

    @abstractmethod
    def create_bucket(self, bucket: str) -> Dict:
        """Creates bucket.

        Args:
            bucket (str): Bucket name to create.

        Returns:
            Dict: Response.
        """
        pass

    @abstractmethod
    def delete_bucket(self, bucket: str) -> Dict:
        """Deletes bucket.

        Args:
            bucket (str): Bucket to delete.

        Returns:
            Dict: Response.
        """

    @abstractmethod
    def delete_objects(self, bucket: str, objects: List[Dict]) -> Dict:
        """Deletes objects in a bucket.

        Args:
            bucket (str): Bucket name.
            objects (List[Dict]): List of objects to delete.

        Returns:
            Dict: Response.
        """
        pass

    @abstractmethod
    def download_file(self, bucket: str, key: str, filename: str) -> None:
        """Downloads a file from a bucket to local.

        Args:
            bucket (str): Bucket name.
            key (str): File name in bucket.
            filename (str): Local file name to store file.
        """
        pass

    @abstractmethod
    def list_buckets(self) -> Dict:
        """Lists all buckets.

        Returns:
            Dict: Response.
        """
        pass

    @abstractmethod
    def list_objects(self, bucket: str) -> Dict:
        """List objects in a bucket.

        Args:
            bucket (str): Bucket name.

        Returns:
            Dict: Response object.
        """
        pass

    @abstractmethod
    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        """Uploads a file from local to a bucket.

        Args:
            filename (str): Local file to upload.
            bucket (str): Bucket name to upload to.
            key (str): Key name to save the file.
        """
        pass


class S3Repository(AbstractObjStore):
    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        endpoint_url: str,
        region_name: str,
        log_info: LogConfig,
    ) -> None:
        """Object storage repository for S3 leveraging the boto3 client.
        For local storage with MinIO, you can set endpoint_url to
        http://localhost:9000.

        Docs on MinIO and boto3:
        https://docs.min.io/docs/how-to-use-aws-sdk-for-python-with-minio-server.html

        Docs for all boto3 methods can be found here:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html

        Args:
            aws_access_key_id (str): AWS Access Key
            aws_secret_access_key (str): AWS Secret Key
            endpoint_url (str): AWS endpoint
            region_name (str): AWS region
            log_info (LogConfig): Logger config
        """

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.log_info = log_info

    @property
    def log(self) -> Logger:
        try:
            return self._log
        except AttributeError:
            self._log = child_logger(self.log_info.log_name, self.__class__.__name__)
            return self._log

    @property
    def client(self) -> boto3.client:
        try:
            return self._client
        except AttributeError:
            self._client = boto3.client(
                "s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name,
            )
            return self._client

    def create_bucket(self, bucket: str) -> Dict:
        self.client.create_bucket(Bucket=bucket)

    def delete_bucket(self, bucket: str) -> Dict:
        self.client.delete_bucket(Bucket=bucket)

    def delete_objects(self, bucket: str, objects: List) -> Dict:
        del_objects = [{"Key": obj} for obj in objects]
        self.client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": del_objects},
        )

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        return self.client.download_file(
            Bucket=bucket,
            Key=key,
            Filename=filename,
        )

    def list_buckets(self) -> Dict:
        return self.client.list_buckets()

    def list_objects(self, bucket: str) -> Dict:
        return self.client.list_objects_v2(Bucket=bucket)

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        return self.client.upload_file(
            Filename=filename,
            Bucket=bucket,
            Key=key,
        )


class ObjStoreRepository:
    _obj_handlers = {
        ObjStoreController.minio: S3Repository,
        ObjStoreController.s3: S3Repository,
    }

    @validate_arguments(config=dict(arbitrary_types_allowed=True))
    def __init__(
        self,
        obj_store_info: Dict,
        obj_handler: ObjStoreController,
        log_info: LogConfig,
    ) -> None:
        """A wrapper class to provide a consistent interface to the
        different ObjStore types found in the _obj_handlers class
        attribute.

        Args:
            obj_store_info (Dict): Info to connect to the object store.
            obj_handler (ObjStoreController): Type of object store to fetch.
        """
        self.obj_store_info = obj_store_info
        self.obj_handler = obj_handler
        self.log_info = log_info

    @property
    def handler(self) -> AbstractObjStore:
        return ObjStoreRepository._obj_handlers[self.obj_handler](
            log_info=self.log_info,
            **self.obj_store_info,
        )
