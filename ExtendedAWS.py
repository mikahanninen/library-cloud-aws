from RPA.Cloud.AWS import AWS, aws_dependency_required
import jmespath
from typing import Optional
from botocore.exceptions import ClientError
from pathlib import Path


class ExtendedAWS(AWS):
    def __init__(self, region: str = ..., robocorp_vault_name: Optional[str] = None):
        super().__init__(region, robocorp_vault_name)

    @aws_dependency_required
    def list2_files(
        self,
        bucket_name: str,
        filter: Optional[str] = None,
        max_keys: Optional[int] = None,
        prefix: Optional[str] = None,
        **kwargs,
    ) -> list:
        """List files in the bucket

        .. note:: This keyword accepts additional parameters in key=value format

        More info on `additional parameters <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2/>`_.

        :param bucket_name: name for the bucket
        :return: list of files
        """  # noqa: E501
        client = self._get_client_for_service("s3")
        paginator = client.get_paginator("list_objects_v2")
        if max_keys:
            kwargs["MaxKeys"] = max_keys
        if prefix:
            kwargs["Prefix"] = prefix
        files = []
        try:
            paginator = paginator.paginate(Bucket=bucket_name, **kwargs)
            if filter:
                filtered = paginator.search(filter)
                for page in filtered:
                    files.append(page)
            else:
                for page in paginator:
                    files.extend(page["Contents"] if "Contents" in page else [])

        except ClientError as e:
            self.logger.error(e)
        return files

    @aws_dependency_required
    def download_files(
        self,
        bucket_name: Optional[str] = None,
        files: Optional[list] = None,
        target_directory: Optional[str] = None,
        **kwargs,
    ) -> list:
        """Download files from bucket to local filesystem

        .. note:: This keyword accepts additional parameters in key=value format.

        More info on `additional parameters <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.download_file/>`_.

        :param bucket_name: name for the bucket
        :param files: list of S3 object names
        :param target_directory: location for the downloaded files, default
            current directory
        :return: number of files downloaded
        """  # noqa: E501
        client = self._get_client_for_service("s3")
        download_count = 0

        for _, object_name in enumerate(files):
            try:
                object_as_path = Path(object_name)
                download_path = str(Path(target_directory) / object_as_path.name)
                response = client.download_file(
                    bucket_name, object_name, download_path, **kwargs
                )
                if response is None:
                    download_count += 1
            except ClientError as e:
                self.logger.error("Download error with '%s': %s", object_name, str(e))

        return download_count
