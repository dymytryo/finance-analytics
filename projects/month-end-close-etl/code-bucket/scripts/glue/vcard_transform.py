"""
vcard_transform.py - External CSV File Processor

This job processes external payment processor files (e.g., NorthCard, FleetPay)
that are delivered to S3 inbound paths. It:

1. Reads CSV files from S3 inbound location
2. Applies schema mapping (column renames, type casts)
3. Adds metadata columns (report month, etl timestamp)
4. Writes to S3 as Parquet
5. Registers/updates table in Glue catalog

This pattern is used for:
- NorthCard VCard transaction files
- FleetPay VCard transaction files
- Adyen payment processor files

Usage:
    aws glue start-job-run \
        --job-name finance-monthly-rpt-northcard-vcard-revenue \
        --arguments '{
            "--config_filename": "finance_config.conf",
            "--code_region": "us-west-2",
            "--processor": "northcard"
        }'

Author: Data Platform Team
"""

import awswrangler as wr
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
from dateutil.relativedelta import relativedelta
import boto3
import sys

from glue_utils import read_config, findFile, get_kms_key, logger


# ============================================================================
# Initialize Spark/Glue Context
# ============================================================================

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


# ============================================================================
# Configuration
# ============================================================================

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "code_region", "config_filename", "processor"],
)
job.init(args["JOB_NAME"], args)

processor = args["processor"]  # 'northcard' or 'fleetpay'
logger.info(f"Processing files for: {processor}")

# Load configuration
conf_filename = findFile(args["config_filename"])
params = read_config(conf_filename)
logger.info(f"Loaded config: {conf_filename}")

# Extract common parameters
destination_bucketname = params.get("Common", "destination_bucketname")
parquet_path = params.get("Common", "parquet_path")
finance_bucketname = params.get("Common", "finance_bucketname")

# Get processor-specific configuration
if processor == "northcard":
    source_path = params.get("Common", "vcard_northcard_src_path")
    columns_str = params.get("Common", "northcard_columns")
    schema_mapping = eval(params.get("Common", "northcard_schema"))
    table_name = "monthly_rpt_northcard_vcard_revenue"
elif processor == "fleetpay":
    source_path = params.get("Common", "vcard_fleetpay_src_path")
    columns_str = params.get("Common", "fleetpay_columns")
    schema_mapping = eval(params.get("Common", "fleetpay_schema"))
    table_name = "monthly_rpt_fleetpay_vcard_revenue"
else:
    raise ValueError(f"Unknown processor: {processor}")

# Calculate report month (previous month)
report_month = (datetime.now() + relativedelta(months=-1)).replace(day=1)
report_month_str = report_month.strftime("%Y-%m")
logger.info(f"Report month: {report_month_str}")

# Get AWS account and KMS key
aws_account = boto3.client("sts").get_caller_identity().get("Account")
kms = get_kms_key(destination_bucketname, aws_account)
kms_id = kms[1]


# ============================================================================
# Helper Functions
# ============================================================================

def apply_schema_mapping(df, schema_mapping):
    """
    Apply schema mapping to DataFrame.

    Schema mapping format:
    [(source_col, source_type, target_col, target_type), ...]

    Example:
    [("Card Last 4", "int", "Card_Last_4", "int"),
     ("Spend", "string", "Spend", "double")]
    """

    for source_col, source_type, target_col, target_type in schema_mapping:
        if source_col in df.columns:
            # Rename column if needed
            if source_col != target_col:
                df = df.withColumnRenamed(source_col, target_col)

            # Cast to target type
            if target_type == "int":
                df = df.withColumn(target_col, F.col(target_col).cast(IntegerType()))
            elif target_type == "double":
                df = df.withColumn(target_col, F.col(target_col).cast(DoubleType()))
            elif target_type == "decimal(38,2)":
                df = df.withColumn(target_col, F.col(target_col).cast(DecimalType(38, 2)))
            elif target_type == "date":
                df = df.withColumn(target_col, F.to_date(F.col(target_col)))
            # string type needs no conversion

    return df


def get_input_files(bucket, prefix, report_month_str):
    """Get list of input files from S3 for the report month."""

    s3_client = boto3.client("s3")

    # Look for files in month-specific folder
    full_prefix = f"{prefix}/{report_month_str}/"
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=full_prefix)

    files = []
    if "Contents" in response:
        for obj in response["Contents"]:
            key = obj["Key"]
            if key.endswith(".csv") or key.endswith(".CSV"):
                files.append(f"s3://{bucket}/{key}")

    return files


# ============================================================================
# Main Processing
# ============================================================================

# Find input files
input_files = get_input_files(finance_bucketname, source_path, report_month_str)

if not input_files:
    logger.warning(f"No input files found for {processor} in {report_month_str}")
    logger.info("Job completed with no files to process")
    job.commit()
    sys.exit(0)

logger.info(f"Found {len(input_files)} input files")
for f in input_files:
    logger.info(f"  - {f}")

# Read CSV files
logger.info("Reading CSV files...")
df = spark.read.option("header", "true").csv(input_files)

logger.info(f"Input record count: {df.count()}")
logger.info(f"Input schema: {df.columns}")

# Apply schema mapping
logger.info("Applying schema mapping...")
df = apply_schema_mapping(df, schema_mapping)

# Add metadata columns
df = df.withColumn("reportmonth", F.lit(report_month.date()))
df = df.withColumn("etl_timestamp", F.current_timestamp())
df = df.withColumn("partition_id", F.lit(report_month_str))

logger.info(f"Output schema: {df.columns}")
logger.info(f"Output record count: {df.count()}")

# Write to S3 as Parquet
output_path = f"s3://{destination_bucketname}/{parquet_path}/{table_name}/"
logger.info(f"Writing to: {output_path}")

# Convert to DynamicFrame for Glue catalog registration
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "output")

# Write with partitioning
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": output_path,
        "partitionKeys": ["partition_id"],
    },
    format_options={
        "compression": "snappy",
    },
    transformation_ctx="datasink",
)

# Note: In production, you would also update the Glue catalog table
# using awswrangler or the Glue API

logger.info(f"Successfully processed {processor} files for {report_month_str}")

job.commit()
