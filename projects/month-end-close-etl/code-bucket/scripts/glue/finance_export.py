"""
finance_export.py - Generic Monthly Report Generator

This is a table-driven Glue job that:
1. Reads configuration from a .conf file
2. Determines which SQL file to execute based on table name
3. Checks if today is the correct business day to run
4. Executes SQL via Athena or Starburst
5. Writes results to S3 as Parquet
6. Registers/updates the table in Glue catalog

Usage:
    aws glue start-job-run \
        --job-name finance-monthly-rpt-cash-billed-revenue \
        --arguments '{
            "--table_name": "monthly_rpt_cash_billed_revenue",
            "--config_filename": "finance_config.conf",
            "--code_region": "us-west-2"
        }'

Author: Data Platform Team
"""

import awswrangler as wr
from datetime import datetime, date
import boto3
import sys
import argparse
from awsglue.utils import getResolvedOptions
from dateutil.relativedelta import relativedelta

# Import shared utilities
from glue_utils import read_config, findFile, get_kms_key, run_sql_query, logger


def read_file(filename):
    """Read SQL file contents."""
    with open(filename) as file:
        return file.read()


def run_sql():
    """Execute SQL and write results to S3."""
    logger.info("Executing SQL")

    # Choose execution engine based on command line flag
    if cmd_args.run_athena_query:
        logger.info("Executing Athena SQL")
        logger.info(f"SQL version: {athena_sql_query[:100]}...")
        df = wr.athena.read_sql_query(sql=athena_sql_query, database=nopii_db)
    else:
        logger.info("Executing Starburst SQL")
        df = run_sql_query(athena_sql_query, aws_account)

    # Handle Monthly reports (partitioned by report month)
    if report_type == "Monthly":
        df["partition_id"] = df["reportmonth"].astype(str)
        destination_path = (
            f"s3://{destination_bucketname}/{destination_parquet_path}/{tablename}/"
        )

        logger.info(f"Writing to: {destination_path}")
        wr.s3.to_parquet(
            df=df,
            path=destination_path,
            dataset=True,
            mode=mode,
            max_rows_by_file=100000,
            database=schema_name,
            table=tablename,
            partition_cols=["partition_id"],
            s3_additional_kwargs={
                "ServerSideEncryption": "aws:kms",
                "SSEKMSKeyId": kms_id,
            },
        )
        logger.info(f"Successfully wrote {len(df)} records to {tablename}")

    # Handle Daily reports
    elif partition_column == "partition_id" and report_type == "Daily":
        destination_path = (
            f"s3://{destination_bucketname}/{destination_parquet_path}/{tablename}/"
        )

        if not df.empty:
            logger.info(f"Writing {len(df)} records to: {destination_path}")
            wr.s3.to_parquet(
                df=df,
                path=destination_path,
                dataset=True,
                mode=mode,
                s3_additional_kwargs={
                    "ServerSideEncryption": "aws:kms",
                    "SSEKMSKeyId": kms_id,
                },
                database=schema_name,
                table=tablename,
                dtype=dtype,
                partition_cols=["partition_id"],
            )

            # Optionally write to outbound path for SFTP
            if outbound_path:
                filename_suffix = datetime.now().strftime("%m%d%Y_%H")
                report_month = datetime.now().replace(day=1).date().strftime("%Y-%m")
                outbound_destination = (
                    f"s3://{finance_outbound_bucketname}/internal/outbound/"
                    f"FBO_Accounting/{report_month}/{outbound_path}/"
                    # The file extension was beyond the right edge of the screenshot.
                    f"{outbound_path}_{filename_suffix}.csv"
                )
                logger.info(f"Writing CSV to: {outbound_destination}")
                wr.s3.to_csv(df=df, path=outbound_destination, index=False)
        else:
            logger.info("DataFrame is empty - no records to write")


def business_day_check(business_day):
    """
    Check if today is the Nth business day of the month.
    Business days exclude weekends and US Federal Reserve holidays.
    """
    sql = f"""
    SELECT full_date FROM (
        SELECT
            ROW_NUMBER() OVER (PARTITION BY month_number ORDER BY date_id ASC) AS rank,
            month_number,
            full_date,
            day_of_week,
            holiday_us_federal_reserve
        FROM date_dim
        WHERE full_date >= DATE_TRUNC('month', current_date)
          AND full_date < DATE_TRUNC('month', DATE_ADD('month', 1, current_date))
          AND day_of_week NOT IN ('Saturday', 'Sunday')
          AND holiday_us_federal_reserve = 0
    )
    WHERE rank = {business_day}
    """

    df = wr.athena.read_sql_query(sql=sql, database="bdc_dwh")
    business_date = list(df["full_date"])[0]

    if business_date == date.today():
        logger.info(f"{date.today()} is business day {business_day} - running SQL")
        logger.info(f"SQL file version: {sql_file}")
        run_sql()
    else:
        logger.info(f"{date.today()} is NOT business day {business_day} - skipping")


# ============================================================================
# Main Execution
# ============================================================================

# Parse command line arguments
parser = argparse.ArgumentParser(description="Finance Report Generator")
parser.add_argument(
    "--run_athena_query",
    type=bool,
    default=False,
    help="Use Athena instead of Starburst for SQL execution",
)
cmd_args, unknown = parser.parse_known_args()
logger.info(f"Parsed arguments: {cmd_args}")

# Get Glue job parameters
args = getResolvedOptions(sys.argv, ["table_name", "code_region", "config_filename"])

tablename = args["table_name"]
logger.info(f"Table name: {tablename}")

config_filename = args["config_filename"]
logger.info(f"Config file: {config_filename}")

# Load configuration
conf_filename = findFile(config_filename)
params = read_config(conf_filename)
logger.info(f"Loaded config from: {conf_filename}")

# Extract parameters from config
nopii_db = params.get("Common", "nopii_db")
code_region = params.get("Common", "code_region")
destination_bucketname = params.get("Common", "destination_bucketname")
destination_parquet_path = params.get("Common", "parquet_path")
finance_outbound_bucketname = params.get("Common", "finance_bucketname")

# Get table-specific configuration
partition_column = params.get("Common", f"{tablename}_partition_column")
report_type = params.get("Common", f"{tablename}_report")
sql_file = params.get("Common", f"{tablename}_sql")
mode = (
    params.get("Common", f"{tablename}_mode")
    if params.has_option("Common", f"{tablename}_mode")
    else "overwrite"
)
dtype = (
    eval(params.get("Common", f"{tablename}_dtype"))
    if params.has_option("Common", f"{tablename}_dtype")
    else None
)
outbound_path = params.get(
    "Common", f"{tablename}_output_path", fallback=None
)

# Determine schema name
schema_name = (
    "intermediate"
    if outbound_path
    else params.get("Common", "dwh_db", fallback="bdc_finance")
)

# Get business day lists
day1_tables_list = params.get("Common", "day1_tables_list", fallback="").split(",")
day2_tables_list = params.get("Common", "day2_tables_list", fallback="").split(",")
day3_tables_list = params.get("Common", "day3_tables_list", fallback="").split(",")
day4_tables_list = params.get("Common", "day4_tables_list", fallback="").split(",")

logger.info(f"Partition column: {partition_column}")
logger.info(f"Report type: {report_type}")
logger.info(f"SQL file: {sql_file}")
logger.info(f"Write mode: {mode}")
logger.info(f"Schema: {schema_name}")

# Load SQL file
athena_sql_file = findFile(f"{sql_file}.sql")
athena_sql_query = read_file(athena_sql_file)

# Get AWS account and KMS key
aws_account = boto3.client("sts").get_caller_identity().get("Account")
logger.info(f"AWS Account: {aws_account}")

kms = get_kms_key(destination_bucketname, aws_account)
kms_id = kms[1]

# Execute based on business day configuration
if tablename in day1_tables_list:
    business_day_check(1)
elif tablename in day2_tables_list:
    business_day_check(2)
elif tablename in day3_tables_list:
    business_day_check(3)
elif tablename in day4_tables_list:
    business_day_check(4)
else:
    # No business day restriction - run immediately
    logger.info(f"Running {tablename} without business day check")
    run_sql()
