"""
glue_utils.py - Shared Utility Library for Glue ETL Jobs

This module provides common functions used across all finance Glue jobs:

1. Configuration reading (INI format)
2. S3 operations (copy, delete, check existence)
3. KMS key retrieval
4. SQL execution (Athena and Starburst)
5. Glue catalog operations
6. Decorators (logging, retry, deprecation)

Usage:
    from glue_utils import read_config, findFile, get_kms_key, run_sql_query, logger

Author: Data Platform Team
"""

import datetime
import inspect
import io
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from configparser import ConfigParser, ExtendedInterpolation
from contextlib import suppress
from functools import wraps
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Tuple, Type, Union

import boto3
import boto3.session
import pandas as pd
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError


# ============================================================================
# Logger Setup
# ============================================================================

logger = logging.getLogger()
MSG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"

# Clear existing handlers
if logger.handlers:
    for handler in list(logger.handlers):
        logger.removeHandler(handler)

logging.basicConfig(
    format=MSG_FORMAT,
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)


# ============================================================================
# AWS Clients
# ============================================================================

glue_client = boto3.client("glue", region_name="us-west-2")
athena_client = boto3.client("athena", region_name="us-west-2")
s3_client = boto3.client("s3")


# ============================================================================
# Decorators
# ============================================================================

def deprecated(f):
    """Mark a function as deprecated. Emits warning when called."""

    @wraps(f)
    def func_with_warning(*args, **kwargs):
        warnings.warn(
            f"Call to deprecated function '{f.__name__}' in {os.path.basename(__file__)}",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return f(*args, **kwargs)

    return func_with_warning


def log_signature(f):
    """Log function arguments when called (hides sensitive values)."""

    @wraps(f)
    def wrapper(*args, **kwargs):
        signature = {}

        # Get default values
        for param, arg in inspect.signature(f).parameters.items():
            if arg.default is not inspect.Parameter.empty:
                signature[param] = arg.default
            else:
                signature[param] = None

        # Overwrite with passed kwargs
        signature.update(kwargs)

        # Fill positional args
        sig_keys = list(signature.keys())
        for i in range(len(args)):
            signature[sig_keys[i]] = args[i]

        # Build representation (hide sensitive values)
        sig_repr = []
        sensitive_words = {"secret", "password", "kms", "auth", "token"}
        for param, arg in signature.items():
            if any(word in param.lower() for word in sensitive_words):
                sig_repr.append(f"{param}=*****")
            else:
                sig_repr.append(f"{param}={arg!r}")

        logger.info(f"Called {f.__name__}({', '.join(sig_repr)})")
        return f(*args, **kwargs)

    return wrapper


def retry(
    exceptions: Union[Type[BaseException], Tuple[Type[BaseException], ...]],
    total_tries: int = 4,
    initial_wait: int = 60,
    backoff_factor: Union[int, float] = 3,
):
    """
    Retry decorator with exponential backoff.

    Args:
        exceptions: Exception(s) that trigger a retry
        total_tries: Total number of attempts
        initial_wait: Seconds to sleep before first retry
        backoff_factor: Multiplier for delay each retry
    """

    def retry_decorator(f):
        @wraps(f)
        def func_with_retries(*args, **kwargs):
            _try = 0
            _delay = initial_wait

            while _try <= total_tries:
                try:
                    _try += 1
                    logger.debug(f"Trying {f.__name__}, attempt: {_try}")
                    return f(*args, **kwargs)
                except exceptions as e:
                    msg = f"Exception: {repr(e)}\nFunction: {f.__name__}\n"

                    if _try >= total_tries:
                        msg += f"Failed after {_try} tries."
                        logger.error(msg)
                        raise

                    msg += f"Retrying in {_delay} seconds..."
                    logger.warning(msg)
                    time.sleep(_delay)
                    _delay *= backoff_factor

        return func_with_retries

    return retry_decorator


# ============================================================================
# Configuration Functions
# ============================================================================

def findFile(filename: str) -> str:
    """Find a file in sys.path directories."""

    for dirname in sys.path:
        candidate = os.path.join(dirname, filename)
        if os.path.isfile(candidate):
            return candidate

    raise ValueError(f"Can't find file: {filename}")


def read_config(
    filename: Union[str, os.PathLike],
    interpolation_type: str = "extended",
) -> ConfigParser:
    """Read an INI configuration file."""

    if interpolation_type.lower() == "extended":
        config = ConfigParser(interpolation=ExtendedInterpolation())
    else:
        config = ConfigParser()

    config.read(filename)
    return config


def read_s3_config(
    bucket: str,
    key: str,
    interpolation_type: str = "extended",
) -> ConfigParser:
    """Read a configuration file from S3."""

    s3 = boto3.session.Session().resource("s3")
    s3_bucket = s3.Bucket(bucket)

    with tempfile.NamedTemporaryFile() as temp_file:
        s3_bucket.download_file(key, temp_file.name)
        return read_config(temp_file.name, interpolation_type)


def read_file(filename: Union[str, os.PathLike]) -> str:
    """Read entire contents of a file."""

    with open(filename) as file:
        return file.read()


# ============================================================================
# S3 Operations
# ============================================================================

def get_kms_key(bucket: str, aws_account: str) -> Tuple[str, str]:
    """
    Get KMS key ARN and ID for an S3 bucket.

    Returns:
        Tuple of (kms_arn, kms_id)
    """

    enc = s3_client.get_bucket_encryption(Bucket=bucket)
    rules = enc["ServerSideEncryptionConfiguration"]["Rules"]

    for rule in rules:
        if "ApplyServerSideEncryptionByDefault" in rule:
            if rule["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == "aws:kms":
                kms_string = rule["ApplyServerSideEncryptionByDefault"]["KMSMasterKeyID"]

                if kms_string.startswith("arn:"):
                    kms_arn = kms_string
                    kms_id = kms_arn.split("/")[1]
                else:
                    kms_id = kms_string
                    kms_arn = f"arn:aws:kms:us-west-2:{aws_account}:key/{kms_id}"

                return kms_arn, kms_id

    raise ValueError(f"No KMS key found for bucket: {bucket}")


def split_path(path: str) -> Tuple[str, str]:
    """Split S3 path into bucket and key."""

    if path.startswith("s3://"):
        path = path[len("s3://"):]
    bucketname, *parts = path.split("/")
    rest = "/".join(parts)
    return bucketname, rest


def file_exists(bucketname: str, key: str) -> bool:
    """Check if an S3 key exists."""

    try:
        s3_client.head_object(Bucket=bucketname, Key=key)
        return True
    except ClientError:
        return False


def directory_exists(bucketname: str, prefix: str) -> bool:
    """Check if an S3 prefix has any objects."""

    if prefix[-1] != "/":
        prefix += "/"

    resp = s3_client.list_objects_v2(Bucket=bucketname, Prefix=prefix, MaxKeys=1)
    return resp.get("KeyCount", 0) > 0


@retry(Exception, total_tries=4, initial_wait=240, backoff_factor=1.5)
@log_signature
def sync_s3_folders(source: str, destination: str, kms_id: str) -> None:
    """Sync S3 folders using AWS CLI."""

    cmd = ["aws", "s3", "sync", source, destination, "--sse-kms-key-id", kms_id, "--sse", "aws:kms"]
    process = subprocess.Popen(cmd)
    exit_code = process.wait()

    if exit_code != 0:
        raise Exception(f"Failed to sync {source} to {destination}")


@retry(Exception, total_tries=4, initial_wait=240, backoff_factor=1.5)
def delete_folder(bucketname: str, prefix: str) -> bool:
    """Delete an S3 folder/prefix."""

    # Lines immediately after the signature were not shown; these two setup
    # statements are reconstructed from the visible use of `bucket` below.
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucketname)

    if prefix[-1] == "/":
        prefix = prefix[:-1]

    if len(prefix.split("/")) < 3:
        logger.warning("Safety: Can only delete keys at level >= 3")
        return False

    if not directory_exists(bucketname, prefix):
        return True

    bucket.objects.filter(Prefix=f"{prefix}/").delete()
    return True


def push_to_s3(obj: Any, bucketname: str, obj_key: str, kms_id: str) -> None:
    """Upload an object to S3 with KMS encryption."""

    config = TransferConfig(use_threads=True)

    if isinstance(obj, str):
        content = obj.encode("utf-8")
    else:
        content = json.dumps(obj).encode("utf-8")

    with io.BytesIO(content) as data:
        boto3.client("s3").upload_fileobj(
            Fileobj=data,
            Bucket=bucketname,
            Key=obj_key,
            Config=config,
            ExtraArgs={
                "ServerSideEncryption": "aws:kms",
                "SSEKMSKeyId": kms_id,
            },
        )


# ============================================================================
# SQL Execution
# ============================================================================

QUERY_TIMEOUT = 900  # 15 minutes
QUERY_STATUS_REFRESH_TIME = 20  # seconds


class QueryAthenaException(Exception):
    """Athena query execution failed."""

    pass


def run_sql_query(sql_query: str, aws_account: str) -> pd.DataFrame:
    """
    Execute SQL via Athena or Starburst based on AWS account.

    In Dev (123456789012): Uses Athena
    In LakeDev/LakeProd: Uses Starburst
    """

    if aws_account == "123456789012":
        logger.info("Running query in Athena")
        return run_athena_sql(sql_query)
    else:
        logger.info("Running query in Starburst")
        return run_starburst_sql(sql_query)


def run_athena_sql(sql_query: str, target_db: str = "bdc_nopii") -> pd.DataFrame:
    """Execute SQL via AWS Athena using awswrangler."""

    try:
        import awswrangler as wr
    except ImportError as e:
        logger.error(f"awswrangler not installed: {e}")
        raise

    logger.info(f"Executing SQL: {sql_query[:200]}...")
    df = wr.athena.read_sql_query(sql=sql_query, database=target_db)
    logger.info(f"Query returned {len(df)} rows")
    return df


def run_starburst_sql(sql_query: str) -> pd.DataFrame:
    """Execute SQL via Starburst/Trino."""

    try:
        from trino.dbapi import connect
        from trino.auth import BasicAuthentication
    except ImportError as e:
        logger.error(f"trino package not installed: {e}")
        raise

    # Get credentials from Secrets Manager
    secret_client = boto3.client("secretsmanager", region_name="us-west-2")
    response = secret_client.get_secret_value(
        SecretId="dataplatform-starburst-greatexpectations"
    )
    secret = json.loads(response["SecretString"])

    conn = connect(
        host=secret.get("STARBURST_HOST"),
        port=secret.get("STARBURST_PORT"),
        user=secret.get("STARBURST_USER"),
        auth=BasicAuthentication(
            secret.get("STARBURST_USER"),
            secret.get("STARBURST_PASSWORD"),
        ),
        http_scheme="https",
        catalog="bdc_glue",
        schema="bdc_nopii",
    )

    logger.info(f"Executing SQL: {sql_query[:200]}...")
    df = pd.read_sql(sql_query, conn)
    logger.info(f"Query returned {len(df)} rows")
    return df


# ============================================================================
# Glue Catalog Operations
# ============================================================================

def get_table_details(database: str, table: str) -> bool:
    """Check if a Glue table exists."""

    try:
        glue_client.get_table(DatabaseName=database, Name=table)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            return False
        raise


def glue_get_partitions(database_name: str, table_name: str) -> List[str]:
    """Get all partition values for a Glue table."""

    all_partitions = []
    kwargs = {"DatabaseName": database_name, "TableName": table_name}

    while True:
        resp = glue_client.get_partitions(**kwargs)
        for partition in resp.get("Partitions", []):
            values = partition.get("Values", [])
            if values:
                all_partitions.append(values[0])

        if "NextToken" not in resp:
            break
        kwargs["NextToken"] = resp["NextToken"]

    return all_partitions


def create_update_glue_table_parquet(
    database_name: str,
    table_name: str,
    schema,  # pyspark.sql.types.StructType
    s3_location: str,
    partition_col: Optional[str] = None,
) -> None:
    """Create or update a Glue catalog table for Parquet data."""

    if partition_col == "":
        partition_col = None

    partition_cols = partition_col.split(",") if partition_col else []

    # Build column definitions
    table_columns = []
    partition_keys = []

    for col_ in schema.fieldNames():
        if col_ not in partition_cols:
            table_columns.append({
                "Name": schema[col_].name,
                "Type": schema[col_].dataType.simpleString(),
            })

    for col_name in partition_cols:
        partition_keys.append({"Name": col_name, "Type": "string"})

    table_input = {
        "Name": table_name,
        "Description": table_name,
        "StorageDescriptor": {
            "Columns": table_columns,
            "Location": s3_location,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1", "classification": "parquet"},
            },
        },
        "PartitionKeys": partition_keys,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"EXTERNAL": "TRUE"},
    }

    if get_table_details(database_name, table_name):
        logger.info("Updating existing table")
        glue_client.update_table(DatabaseName=database_name, TableInput=table_input)
    else:
        logger.info("Creating new table")
        glue_client.create_table(DatabaseName=database_name, TableInput=table_input)


def glue_create_partition(
    database_name: str,
    table_name: str,
    partition_values: List[str],
) -> None:
    """Add partitions to a Glue table."""

    response = glue_client.get_table(DatabaseName=database_name, Name=table_name)

    input_format = response["Table"]["StorageDescriptor"]["InputFormat"]
    output_format = response["Table"]["StorageDescriptor"]["OutputFormat"]
    table_location = response["Table"]["StorageDescriptor"]["Location"]
    serde_info = response["Table"]["StorageDescriptor"]["SerdeInfo"]
    partition_key = response["Table"]["PartitionKeys"][0]["Name"]

    if table_location[-1] != "/":
        table_location += "/"

    for partition_value in partition_values:
        try:
            glue_client.create_partition(
                DatabaseName=database_name,
                TableName=table_name,
                PartitionInput={
                    "Values": [partition_value],
                    "StorageDescriptor": {
                        "Location": f"{table_location}{partition_key}={partition_value}",
                        "InputFormat": input_format,
                        "OutputFormat": output_format,
                        "SerdeInfo": serde_info,
                    },
                },
            )
        except Exception as e:
            logger.warning(f"Error creating partition {partition_value}: {e}")


# ============================================================================
# Helper Functions
# ============================================================================

def divide_chunks(lst: list, n: int) -> Iterator[List[Any]]:
    """Yield chunks of size n from list."""

    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def process_list(
    func: Callable,
    lst: Iterable,
    max_workers: int = 8,
    **kwargs,
) -> List[Any]:
    """Process a list in parallel using ThreadPoolExecutor."""

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(func, elm, **kwargs) for elm in lst]
        for future in as_completed(futures):
            results.append(future.result())
    return results


def get_secret(
    secret_name: str,
    region_name: str = "us-west-2",
) -> Union[str, bytes]:
    """Retrieve a secret from AWS Secrets Manager."""

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)

    if "SecretString" in response:
        return response["SecretString"]
    return response["SecretBinary"]


def str_to_bool(val: str) -> bool:
    """Convert string to boolean."""

    val = val.lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return True
    if val in ("n", "no", "f", "false", "off", "0"):
        return False
    raise ValueError(f"Cannot convert '{val}' to boolean")
