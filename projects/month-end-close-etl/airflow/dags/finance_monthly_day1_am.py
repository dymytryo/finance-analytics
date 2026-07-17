"""
finance_monthly_day1_am.py - Example Airflow DAG for Day 1 Monthly Reports

This DAG demonstrates the patterns used to orchestrate finance ETL jobs:

1. Configuration-driven job definitions with dependencies
2. BDC dependency checks (verify upstream tables are fresh)
3. Upstream/downstream task chaining
4. Datadog tagging for monitoring
5. Business day scheduling (runs on 1st of month at 8am PST)

Schedule: "00 15 1 * *" = 15:00 UTC = 8:00 AM PST on the 1st of each month

Author: Data Platform Team
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import time

# Import utility functions (these would be in your utilities folder)
# from utilities.aws_utils import run_glue_job, glue_job_check, DatadogCustomTag, DatadogJobTier, DatadogJobType
# from utilities.dag_factory import add_python_task, create_dag


# ============================================================================
# Configuration
# ============================================================================

# Job configuration with dependencies
# This dict drives all task creation and wiring
conf = {
    "finance-monthly-rpt-cash-billed-revenue": {
        "bdc_dependency": [
            "bdc-orgstatement-delta-oracle",
            "bdc-orgstatementlineitem-delta-oracle",
            "bdc-billingptpay-full-oracle",
            "bdc-achbillinginfo-full-oracle",
            "bdc-organization-full-oracle",
            "bdc-priceplan-full-oracle",
        ],
        "upstream_dependency": [
            "product-sku-dim-monthly",
        ],
        "downstream_dependency": [
            "finance-monthly-rpt-intuit-revenue",
            "finance-monthly-outbound-billed-je",
        ],
    },
    "finance-monthly-ach-credit-refunds": {
        "bdc_dependency": [
            "bdc-achbillinginfo-full-oracle",
            "bdc-orgstatementlineitem-delta-oracle",
            "bdc-orgstatement-delta-oracle",
        ],
        "downstream_dependency": [
            "finance-monthly-outbound-achcredit-refund-je",
        ],
    },
    "finance-monthly-ach-chargebacks": {
        "bdc_dependency": [
            "bdc-achbillinginfo-full-oracle",
        ],
    },
    "finance-monthly-rpt-creditcard-refunds-revenue": {
        "bdc_dependency": [
            "bdc-achbillinginfo-full-oracle",
            "bdc-orgadditionaldata-full-oracle",
            "bdc-orgstatement-delta-oracle",
            "bdc-billingptpay-full-oracle",
            "bdc-opuser-full-oracle",
        ],
        "downstream_dependency": [
            "finance-monthly-outbound-creditcard-refund-je",
        ],
    },
    "finance-monthly-rpt-partner-org-activity": {
        "bdc_dependency": [
            "bdc-organization-full-oracle",
            "bdc-moneyout-delta-oracle",
            "bdc-invoicemailqueue-full-oracle",
            "bdc-invoice-delta-oracle",
            "bdc-sentpay-delta-oracle",
            "bdc-receivedpay-delta-oracle",
        ],
    },
}


# DAG settings
DAG_NAME = "finance-mrr-day1-am"
SCHEDULE = "00 15 1 * *"  # 8am PST on 1st of month (15:00 UTC)

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime.datetime(year=2023, month=5, day=1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=5),
}


# ============================================================================
# Stub Functions (replace with actual implementations)
# ============================================================================

def run_glue_job(job_name: str, **kwargs):
    """
    Run an AWS Glue job and wait for completion.

    In production, this would:
    1. Call glue_client.start_job_run()
    2. Poll for completion
    3. Handle timeouts and failures

    Args:
        job_name: Name of the Glue job to run
        **kwargs: Additional arguments (custom_datadog_tags, etc.)
    """
    import boto3
    import time

    glue_client = boto3.client("glue", region_name="us-west-2")

    print(f"Starting Glue job: {job_name}")

    # Start job run
    response = glue_client.start_job_run(JobName=job_name)
    run_id = response["JobRunId"]

    print(f"Job run started: {run_id}")

    # Poll for completion
    while True:
        status = glue_client.get_job_run(JobName=job_name, RunId=run_id)
        state = status["JobRun"]["JobRunState"]

        if state in ["SUCCEEDED"]:
            print(f"Job {job_name} completed successfully")
            return

        if state in ["FAILED", "STOPPED", "TIMEOUT"]:
            error_msg = status["JobRun"].get("ErrorMessage", "Unknown error")
            raise Exception(f"Job {job_name} failed: {state} - {error_msg}")

        print(f"Job {job_name} state: {state}")
        time.sleep(30)


def glue_job_check(job_name: str, **kwargs):
    """
    Check if a dependent Glue job has completed recently.

    This verifies that upstream data is fresh before running reports.

    In production, this would:
    1. Query Glue job run history
    2. Check for successful run within time window
    3. Fail if dependency not met

    Args:
        job_name: Name of the Glue job to check
    """
    import boto3
    from datetime import datetime, timedelta

    glue_client = boto3.client("glue", region_name="us-west-2")

    print(f"Checking dependency: {job_name}")

    # Get recent job runs
    response = glue_client.get_job_runs(JobName=job_name, MaxResults=5)

    # Check for successful run in last 24 hours
    cutoff_time = datetime.now() - timedelta(hours=24)

    for run in response.get("JobRuns", []):
        if run["JobRunState"] == "SUCCEEDED":
            completed_on = run.get("CompletedOn")
            if completed_on and completed_on.replace(tzinfo=None) > cutoff_time:
                print(f"Found successful run for {job_name} at {completed_on}")
                return

    raise Exception(f"No recent successful run found for {job_name}")


# ============================================================================
# Datadog Tags (for monitoring)
# ============================================================================

class DatadogCustomTag:
    TABLE_NAME = "table_name"
    JOB_TYPE = "job_type"
    JOB_TIER = "job_tier"
    TARGET = "target"


class DatadogJobType:
    MONTHLY = "monthly"
    DAILY = "daily"


class DatadogJobTier:
    TIER0 = "tier0"  # Critical
    TIER2 = "tier2"  # Important
    TIER3 = "tier3"  # Standard


# ============================================================================
# DAG Definition
# ============================================================================

dag = DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval=SCHEDULE,
    catchup=False,
    tags=["mrr", "finance", "bdc_dependent"],
)


# ============================================================================
# Task Creation
# ============================================================================

# Step 1: Create unique set of BDC dependency check tasks
all_bdc_jobs = set()
for job_config in conf.values():
    all_bdc_jobs.update(job_config.get("bdc_dependency", []))

check_bdc_tasks = {}
for bdc_job in all_bdc_jobs:
    task_name = f"check_{bdc_job}"
    check_bdc_tasks[task_name] = PythonOperator(
        task_id=task_name,
        python_callable=glue_job_check,
        op_kwargs={"job_name": bdc_job},
        retries=2,
        dag=dag,
    )

# Step 2: Create report job tasks and wire dependencies
for glue_job, job_config in conf.items():
    table_name = glue_job[8:].replace("-", "_")  # Remove 'finance-' prefix

    # Create main report task
    report_task = PythonOperator(
        task_id=f"run_{glue_job}",
        python_callable=run_glue_job,
        op_kwargs={
            "job_name": glue_job,
            "custom_datadog_tags": [
                f"{DatadogCustomTag.TABLE_NAME}:{table_name}",
                f"{DatadogCustomTag.JOB_TYPE}:{DatadogJobType.MONTHLY}",
                f"{DatadogCustomTag.JOB_TIER}:{DatadogJobTier.TIER0}",
            ],
        },
        dag=dag,
    )

    # Get BDC dependency check tasks
    bdc_check_tasks = [
        check_bdc_tasks[f"check_{bdc_job}"]
        for bdc_job in job_config.get("bdc_dependency", [])
        if f"check_{bdc_job}" in check_bdc_tasks
    ]

    # Handle jobs with upstream dependencies
    if "upstream_dependency" in job_config:
        for upstream_job in job_config["upstream_dependency"]:
            upstream_task = PythonOperator(
                task_id=f"run_{upstream_job}",
                python_callable=run_glue_job,
                op_kwargs={
                    "job_name": upstream_job,
                    "custom_datadog_tags": [
                        f"{DatadogCustomTag.JOB_TYPE}:{DatadogJobType.MONTHLY}",
                        f"{DatadogCustomTag.JOB_TIER}:{DatadogJobTier.TIER2}",
                    ],
                },
                dag=dag,
            )

            # Wire: BDC checks >> upstream >> report
            for check_task in bdc_check_tasks:
                check_task >> upstream_task
            upstream_task >> report_task
    else:
        # No upstream - wire BDC checks directly to report
        for check_task in bdc_check_tasks:
            check_task >> report_task

    # Handle downstream dependencies
    if "downstream_dependency" in job_config:
        # Add delay task (sometimes needed for Starburst table refresh)
        def sleep_five_minutes():
            time.sleep(300)

        sleep_task = PythonOperator(
            task_id=f"sleep_before_{glue_job}_downstream",
            python_callable=sleep_five_minutes,
            dag=dag,
        )

        report_task >> sleep_task

        for downstream_job in job_config["downstream_dependency"]:
            downstream_task = PythonOperator(
                task_id=f"run_{downstream_job}",
                python_callable=run_glue_job,
                op_kwargs={
                    "job_name": downstream_job,
                    "custom_datadog_tags": [
                        f"{DatadogCustomTag.JOB_TIER}:{DatadogJobTier.TIER3}",
                        f"{DatadogCustomTag.JOB_TYPE}:{DatadogJobType.MONTHLY}",
                        f"{DatadogCustomTag.TARGET}:sftp_corp_a_bill_com",
                    ],
                },
                dag=dag,
            )

            sleep_task >> downstream_task
