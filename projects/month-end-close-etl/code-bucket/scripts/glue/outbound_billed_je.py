"""
outbound_billed_je.py - Journal Entry File Generator

This job generates NetSuite-compatible journal entry CSV files from
monthly revenue report data. Key features:

1. Queries the monthly_rpt_cash_billed_revenue table
2. Batches records into files of max 9998 records (NetSuite limit is 9999)
3. Generates summary (debit) and detail (credit) lines for each file
4. Creates a summary tracking table for audit purposes
5. Outputs CSV files to S3 for SFTP pickup

Usage:
    aws glue start-job-run \
        --job-name finance-monthly-outbound-billed-je \
        --arguments '{
            "--config_filename": "finance_config.conf",
            "--code_region": "us-west-2"
        }'

Author: Data Platform Team
"""

import awswrangler as wr
from datetime import datetime
import boto3
import sys
from awsglue.utils import getResolvedOptions
from dateutil.relativedelta import relativedelta

from glue_utils import read_config, findFile, get_kms_key, logger


# ============================================================================
# Configuration
# ============================================================================

args = getResolvedOptions(sys.argv, ["code_region", "config_filename"])

config_filename = args["config_filename"]
logger.info(f"Config file: {config_filename}")

conf_filename = findFile(config_filename)
params = read_config(conf_filename)
logger.info(f"Loaded config: {conf_filename}")

# Extract parameters
nopii_db = params.get("Common", "nopii_db")
stage_bucketname = params.get("Common", "destination_bucketname")
finance_outbound_bucketname = params.get("Common", "finance_bucketname")
stage_parquet_path = params.get("Common", "parquet_path")
internal_outbound_path = params.get("Common", "internal_outbound_path")

# Calculate report month (previous month)
report_month = (
    datetime.now() + relativedelta(months=-1)
).replace(day=1).date().strftime("%Y-%m")
reportmonth_suffix = (
    datetime.now() + relativedelta(months=-1)
).replace(day=1).date().strftime("%B%Y")

logger.info(f"Report month: {report_month}")
logger.info(f"Report month suffix: {reportmonth_suffix}")


# ============================================================================
# Step 1: Create Stage Table with File Batch Numbers
# ============================================================================

# SQL to assign file batch numbers
# NetSuite has a 9999 record limit per file, we use 9998 to be safe
athena_stage_sql = """
WITH config_batch_size AS (
    SELECT 9998 AS batch_size
),
config_month_filter AS (
    SELECT DATE_TRUNC('month', DATE_ADD('month', -1, current_date)) AS report_month
),
config_sku_exclusions AS (
    SELECT 9999 AS sku_ex
    UNION
    SELECT 92 AS sku_ex
),
-- AmEx transactions (separate batching)
amex_summary AS (
    SELECT
        mf.report_month,
        organization_id,
        DATE(payment_date) AS payment_date,
        sku,
        partner_type AS partner,
        SUM(CAST(amount AS DECIMAL(33,2))) AS amount
    FROM bdc_finance.monthly_rpt_cash_billed_revenue t
    INNER JOIN config_month_filter mf ON mf.report_month = t.reportmonth
    WHERE partner_type = 'AmEx'
      AND sku NOT IN (SELECT sku_ex FROM config_sku_exclusions)
    GROUP BY 1, 2, 3, 4, 5
    HAVING SUM(CAST(amount AS DECIMAL(33,2))) <> 0
),
-- Non-AmEx transactions (separate batching)
nonamex_summary AS (
    SELECT
        mf.report_month,
        organization_id,
        DATE(payment_date) AS payment_date,
        sku,
        'Non-AmEx' AS partner,
        SUM(CAST(amount AS DECIMAL(33,2))) AS amount
    FROM bdc_finance.monthly_rpt_cash_billed_revenue t
    INNER JOIN config_month_filter mf ON mf.report_month = t.reportmonth
    WHERE partner_type <> 'AmEx'
      AND sku NOT IN (SELECT sku_ex FROM config_sku_exclusions)
    GROUP BY 1, 2, 3, 4, 5
    HAVING SUM(CAST(amount AS DECIMAL(33,2))) <> 0
),
-- Assign file numbers to AmEx (files 1, 2, ...)
cte_amex AS (
    SELECT
        (ROW_NUMBER() OVER (ORDER BY sku) - 1) % fs.batch_size AS rk,
        ROW_NUMBER() OVER (ORDER BY sku) AS rk_ref,
        t.*
    FROM amex_summary t
    CROSS JOIN config_batch_size fs
),
cte_amex_batches AS (
    SELECT
        DENSE_RANK() OVER (ORDER BY rk_ref - rk) AS file_no,
        t.*
    FROM cte_amex t
),
-- Assign file numbers to Non-AmEx (continuing from AmEx file numbers)
cte_nonamex AS (
    SELECT
        (ROW_NUMBER() OVER (ORDER BY sku) - 1) % fs.batch_size AS rk,
        ROW_NUMBER() OVER (ORDER BY sku) AS rk_ref,
        t.*
    FROM nonamex_summary t
    CROSS JOIN config_batch_size fs
),
cte_nonamex_batches AS (
    SELECT
        (DENSE_RANK() OVER (ORDER BY rk_ref - rk))
            + COALESCE(mx.max_fileno, 0) AS file_no,
        t.*
    FROM cte_nonamex t
    CROSS JOIN (SELECT MAX(file_no) AS max_fileno FROM cte_amex_batches) mx
)
-- Union AmEx and Non-AmEx batches
SELECT
    report_month, organization_id, payment_date, partner, sku, amount, file_no
FROM cte_amex_batches
UNION ALL
SELECT
    report_month, organization_id, payment_date, partner, sku, amount, file_no
FROM cte_nonamex_batches
"""

# Get KMS key for encrypted writes
aws_account = boto3.client("sts").get_caller_identity().get("Account")
logger.info(f"AWS Account: {aws_account}")

kms = get_kms_key(stage_bucketname, aws_account)
kms_id = kms[1]

# Execute staging query
destination_path = (
    f"s3://{stage_bucketname}/{stage_parquet_path}/stage_cashrevenue_filebatches/"
)
logger.info(f"Stage table path: {destination_path}")

df = wr.athena.read_sql_query(sql=athena_stage_sql, database=nopii_db)

if df.empty:
    raise Exception("Empty dataframe - no records to process")

# Get distinct file numbers
file_numbers = df.file_no.unique()
logger.info(f"Total files to create: {len(file_numbers)}")

# Write stage table
wr.s3.to_parquet(
    df=df,
    path=destination_path,
    dataset=True,
    mode="overwrite",
    max_rows_by_file=100000,
    s3_additional_kwargs={
        "ServerSideEncryption": "aws:kms",
        "SSEKMSKeyId": kms_id,
    },
    database="bdc_finance",
    table="stage_cashrevenue_filebatches",
)
logger.info("Stage table written successfully")


# ============================================================================
# Step 2: Create Summary Tracking Table
# ============================================================================

summary_sql = """
SELECT
    report_month AS reportmonth,
    'Billed_JE' AS je_file_name,
    file_no AS je_file_no,
    SUM(amount) AS journalitemline_debitamount,
    SUM(amount) AS journalitemline_creditamount,
    0 AS delta,
    LOCALTIMESTAMP AS etl_timestamp,
    CAST(report_month AS VARCHAR) AS partition_id
FROM bdc_finance.stage_cashrevenue_filebatches
GROUP BY 1, 2, 3
"""

summary_destination = (
    f"s3://{stage_bucketname}/{stage_parquet_path}/"
    "monthly_rpt_journal_entry_summary_revenue/"
)
summary_df = wr.athena.read_sql_query(sql=summary_sql, database=nopii_db)

wr.s3.to_parquet(
    df=summary_df,
    path=summary_destination,
    dataset=True,
    mode="append",
    max_rows_by_file=100000,
    partition_cols=["partition_id"],
    s3_additional_kwargs={
        "ServerSideEncryption": "aws:kms",
        "SSEKMSKeyId": kms_id,
    },
    database="bdc_finance",
    table="monthly_rpt_journal_entry_summary_revenue",
)
logger.info("Summary table updated")


# ============================================================================
# Step 3: Generate Individual JE Files
# ============================================================================

logger.info("Generating Billed JE Files...")

for file_num in file_numbers:
    logger.info(f"Processing file {file_num}")

    # SQL to generate NetSuite-formatted JE file
    je_sql = f"""
WITH product_sku_dim AS (
    SELECT
        sku,
        sku_product_description,
        netsuite_gl_account_number,
        product_group,
        CASE
            WHEN LENGTH(
                TRIM(
                    SUBSTRING(
                        netsuite_sku,
                        1,
                        STRPOS(REPLACE(netsuite_sku, ' - ', '--'), '-') - 1
                    )
                )
            ) <= 3
                THEN sku_product_description
            ELSE TRIM(
                SUBSTRING(
                    netsuite_sku,
                    1,
                    STRPOS(REPLACE(netsuite_sku, ' - ', '--'), '-') - 1
                )
            )
        END AS sku_num_desc,
        TRIM(
            SUBSTRING(
                netsuite_sku,
                1,
                STRPOS(REPLACE(netsuite_sku, ' - ', '--'), '-') - 1
            )
        ) AS sku_num
    FROM bdc_dwh.product_sku_dim
    WHERE sku IS NOT NULL
),
config_amex_gl AS (
    SELECT 'Subscription Revenue' AS product_group, '45010' AS gl_acct_ref
    UNION SELECT 'Transaction Revenue', '45020'
),
config_amex_item AS (
    SELECT 'Subscription Revenue' AS product_group, 'SKU-Bank Sub' AS item_ref
    UNION SELECT 'Transaction Revenue', 'SKU-Bank Tran'
),
file_data AS (
    -- Summary row (debit entry)
    SELECT
        CONCAT(
            'Billed Revenue JE ', CAST(file_no AS VARCHAR),
            ' - ', DATE_FORMAT(report_month, '%b %Y')
        ) AS tranid,
        'Your Company Name' AS subsidiary,
        'USD' AS currency,
        '1' AS exchangerate,
        report_month AS postingperiod,
        DATE_ADD('day', -1, DATE_ADD('month', 1, report_month)) AS tranDate,
        '11181' AS journalItemLine_accountRef,
        SUM(amount) AS journalItemLine_debitAmount,
        CAST(NULL AS DECIMAL(33, 2)) AS journalItemLine_creditAmount,
        CASE
            WHEN partner = 'AmEx' THEN CONCAT(
                partner, ' Billed Revenue - ', DATE_FORMAT(report_month, '%b %Y')
            )
            ELSE CONCAT('Billed Revenue - ', DATE_FORMAT(report_month, '%b %Y'))
        END AS journalItemLine_memo,
        '' AS journalItemLine_entityRef,
        CAST(NULL AS TIMESTAMP) AS journalItemLine_startdate,
        CAST(NULL AS TIMESTAMP) AS journalItemLine_enddate,
        '' AS journalItemLine_taxCodeRef,
        CAST(NULL AS DOUBLE) AS Quantity,
        '' AS Item_Reference,
        0 AS rk
    FROM bdc_finance.stage_cashrevenue_filebatches t
    WHERE t.file_no = {file_num}
    /*
     * The supplied screenshots skip the remainder of the summary GROUP BY and
     * the complete detail-credit SELECT/UNION section here. That original SQL
     * remains to be inserted when the missing lines are provided.
     */
)
SELECT *
FROM file_data
ORDER BY rk
"""

    df_output = wr.athena.read_sql_query(sql=je_sql, database=nopii_db)

    # Rename columns to NetSuite format
    df_output.rename(
        columns={
            "tranid": "tranId",
            "trandate": "tranDate",
            "journalitemline_accountref": "journalItemLine_accountRef",
            "journalitemline_debitamount": "journalItemLine_debitAmount",
            "journalitemline_creditamount": "journalItemLine_creditAmount",
            "journalitemline_memo": "journalItemLine_memo",
            "journalitemline_entityref": "journalItemLine_entityRef",
            "journalitemline_startdate": "journalItemLine_startdate",
            "journalitemline_enddate": "journalItemLine_enddate",
            "journalitemline_taxcoderef": "journalItemLine_taxCodeRef",
            "quantity": "Quantity",
            "item_reference": "Item Reference",
        },
        inplace=True,
    )

    # Sort by rk (summary row first) and drop rk column
    final_df = df_output.sort_values(by="rk")
    final_df = final_df.drop("rk", axis=1)

    # Get column list for CSV output
    columns_list = list(final_df.columns)
    logger.info(f"File {file_num} columns: {columns_list}")

    # Write CSV to outbound path
    outbound_path = (
        f"s3://{finance_outbound_bucketname}/{internal_outbound_path}/"
        f"{report_month}/journal_entry/billed_revenue/Billed_JE_{file_num}.csv"
    )
    logger.info(f"Writing: {outbound_path}")

    wr.s3.to_csv(
        df=final_df,
        path=outbound_path,
        index=False,
        columns=columns_list,
    )

logger.info(f"Successfully generated {len(file_numbers)} JE files")
