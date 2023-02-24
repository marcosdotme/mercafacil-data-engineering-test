from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import List

import boto3


def upload_files_to_s3_bucket(
    bucket_name: str,
    bucket_folder: str,
    files: str | List[str],
    access_key: str = None,
    secret_access_key: str = None
) -> None:
    """Upload files to AWS S3 bucket.

    Arguments
    ---------
        bucket_name `str`: Bucket name.
        bucket_folder `str`: Folder on S3 bucket to upload file.
        files `str | List[str]`: Files to upload.

    Keyword Arguments
    -----------------
        access_key `str`: AWS Access Key (default: None)
        secret_access_key `str`: AWS Secret Access Key (default: None)

    Example usage
    -------------
    >>> upload_files_to_s3_bucket(
        bucket_name='datalake',
        bucket_folder='bronze',
        files=['data1.csv', 'data2.csv'],
        access_key='MY_AWS_ACCESS_KEY',
        secret_access_key='MY_AWS_SECRET_ACCESS_KEY'
    )
    """

    if isinstance(files, str):
        files = list(files)
    
    if access_key and secret_access_key:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key
        )
    else:
        s3_client = boto3.client('s3')

    def _upload(_file: str) -> None:
        s3_client.upload_file(
            Filename=_file,
            Bucket=bucket_name,
            Key=f"{Path(bucket_folder)}/{Path(_file).name}"
        )

    pool = ThreadPool(processes=len(files))
    pool.map(_upload, files)
