-- ============================================================================
-- monthly_rpt_example.sql - Example Monthly Revenue Report SQL
--
-- This SQL demonstrates the common patterns used in finance report queries:
-- 1. Date range filtering using CTEs
-- 2. Multi-source joins (statements, billing, organizations)
-- 3. Aggregation with HAVING to exclude zero amounts
-- 4. Partner type and SKU-level breakdowns
-- 5. Metadata columns for tracking
--
-- Usage: This file is loaded by finance_export.py based on config mapping
-- ============================================================================

-- ============================================================================
-- Configuration CTEs
-- ============================================================================

WITH report_month_dates AS (
    -- Calculate the date range for the previous month
    -- Jobs run on business day 1-4 of current month for previous month data
    SELECT
        DATE_TRUNC('month', DATE_ADD('month', -1, current_date)) AS month_start,
        DATE_TRUNC('month', current_date) AS month_end
),

-- ============================================================================
-- Source Data CTEs
-- ============================================================================

-- Filter to paid statements only, non-zero amounts

-- [PARSE NOTE: the remainder of this SQL file has not yet been supplied.]
