-- ========================================
-- Project: Financial Market Analysis
-- Objective: Clean and analyze historical financial data to uncover patterns 
-- in trading volumes, price behavior, macroeconomic indicators, and stock indices.
-- ========================================

-- ========================================
-- Set working schema
-- Purpose: Direct SQL operations to the relevant schema for the project
-- ========================================
SET search_path TO "Project";


-- ========================================
-- Preview the dataset
-- Purpose: Get a quick look at the first 10 rows of the original dataset
-- ========================================
SELECT * 
FROM "Project".finance_dataset
LIMIT 10;

-- ========================================
-- Create a cleaned copy of the dataset
-- Purpose: Preserve original data and use a working copy for cleaning and analysis
-- ========================================
CREATE TABLE finance_dataset_clean AS
SELECT * FROM finance_dataset;

-- ========================================
-- Convert the "Date" column to DATE type
-- Purpose: Enable time-based analysis and date extraction
-- ========================================
ALTER TABLE finance_dataset_clean
ALTER COLUMN "Date" TYPE DATE USING "Date"::DATE;

-- ========================================
-- Add extracted time features: year and month
-- Purpose: Support temporal grouping in analysis
-- ========================================
ALTER TABLE finance_dataset_clean
ADD COLUMN year INT,
ADD COLUMN month INT;

UPDATE finance_dataset_clean
SET year = EXTRACT(YEAR FROM "Date"),
    month = EXTRACT(MONTH FROM "Date");

-- ========================================
-- Convert financial columns to NUMERIC for precision
-- Purpose: Enable mathematical operations and rounding
-- ========================================
ALTER TABLE finance_dataset_clean
ALTER COLUMN "Open Price" TYPE NUMERIC(10,2) USING "Open Price"::NUMERIC,
ALTER COLUMN "Close Price" TYPE NUMERIC(10,2) USING "Close Price"::NUMERIC,
ALTER COLUMN "Daily High" TYPE NUMERIC(10,2) USING "Daily High"::NUMERIC,
ALTER COLUMN "Daily Low" TYPE NUMERIC(10,2) USING "Daily Low"::NUMERIC;

-- ========================================
-- Add derived columns: price spread and volatility percentage
-- Purpose: Enhance dataset with metrics that reflect price behavior
-- ========================================
ALTER TABLE finance_dataset_clean
ADD COLUMN price_spread NUMERIC(10,2),
ADD COLUMN volatility_pct NUMERIC(5,2);

UPDATE finance_dataset_clean
SET 
    price_spread = "Daily High" - "Daily Low",
    volatility_pct = ROUND(("Daily High" - "Daily Low") / NULLIF("Open Price", 0) * 100, 2);

-- ========================================
-- Purpose: Track average stock index values over time
-- ========================================
SELECT 
    DATE_TRUNC('month', "Date") AS month,
    ROUND(AVG("Close Price"), 2) AS avg_close_price
FROM finance_dataset_clean
GROUP BY DATE_TRUNC('month', "Date")
ORDER BY month;

-- ========================================
-- Question: When were financial markets most volatile?
-- ========================================
SELECT 
    DATE_TRUNC('month', "Date") AS month,
    ROUND(AVG("Daily High" - "Daily Low"), 2) AS avg_spread
FROM finance_dataset_clean
GROUP BY DATE_TRUNC('month', "Date")
ORDER BY month;

-- ========================================
-- Top 10 highest crude oil prices
-- Purpose: Identify the peak oil price days in the dataset
-- ========================================
SELECT "Date", "Crude Oil Price (USD per Barrel)"
FROM finance_dataset_clean
ORDER BY "Crude Oil Price (USD per Barrel)" DESC
LIMIT 10;

-- ========================================
-- Highest trading volume by month
-- Purpose: Identify periods with the most active trading
-- ========================================
SELECT
    DATE_TRUNC('month', "Date") AS month,
    SUM("Trading Volume") AS total_volume
FROM finance_dataset_clean
GROUP BY DATE_TRUNC('month', "Date")
ORDER BY total_volume DESC
LIMIT 5;

-- ========================================
-- Average GDP growth and unemployment rate by month
-- Purpose: Track macroeconomic performance over time
-- ========================================
SELECT 
    DATE_TRUNC('month', "Date") AS month,
    ROUND(AVG("GDP Growth (%)")::NUMERIC, 2) AS avg_gdp_growth,
    ROUND(AVG("Unemployment Rate (%)")::NUMERIC, 2) AS avg_unemployment
FROM finance_dataset_clean
GROUP BY DATE_TRUNC('month', "Date")
ORDER BY month;

-- ========================================
-- Average inflation and interest rates by month
-- Purpose: Examine monthly economic policy trends
-- ========================================
SELECT 
    DATE_TRUNC('month', "Date") AS month,
    ROUND(AVG("Inflation Rate (%)")::NUMERIC, 2) AS avg_inflation,
    ROUND(AVG("Interest Rate (%)")::NUMERIC, 2) AS avg_interest_rate
FROM finance_dataset_clean
GROUP BY DATE_TRUNC('month', "Date")
ORDER BY month;

-- ========================================
-- Average close price by weekday
-- Purpose: Discover which day of the week sees the highest prices
-- ========================================
SELECT 
    TO_CHAR("Date", 'Day') AS weekday,
    ROUND(AVG("Close Price")::NUMERIC, 2) AS avg_close_price
FROM finance_dataset_clean
GROUP BY TO_CHAR("Date", 'Day')
ORDER BY avg_close_price DESC;

-- ========================================
-- Maximum daily price spread by month
-- Purpose: Measure the largest daily volatility each month
-- ========================================
SELECT 
    DATE_TRUNC('month', "Date") AS month,
    MAX("Daily High" - "Daily Low") AS max_spread
FROM finance_dataset_clean
GROUP BY DATE_TRUNC('month', "Date")
ORDER BY month;

-- ========================================
-- Relationship between interest and inflation rates
-- Purpose: Understand how interest rate trends align with inflation
-- ========================================
SELECT 
    DATE_TRUNC('month', "Date") AS month,
    ROUND(AVG("Interest Rate (%)")::NUMERIC, 2) AS avg_interest,
    ROUND(AVG("Inflation Rate (%)")::NUMERIC, 2) AS avg_inflation
FROM finance_dataset_clean
GROUP BY DATE_TRUNC('month', "Date")
ORDER BY month;

-- ========================================
-- Top 10 companies by average closing price
-- Purpose: Identify the highest-valued companies in the dataset
-- ========================================
SELECT 
    "Company", 
    ROUND(AVG("Close Price")::NUMERIC, 2) AS avg_close_price
FROM finance_dataset_clean
GROUP BY "Company"
ORDER BY avg_close_price DESC
LIMIT 10;

-- ========================================
-- Most traded stock indexes
-- Purpose: Determine which indexes attract the most volume
-- ========================================
SELECT 
    "Stock Index", 
    ROUND(AVG("Trading Volume")::NUMERIC, 2) AS avg_trading_volume
FROM finance_dataset_clean
GROUP BY "Stock Index"
ORDER BY avg_trading_volume DESC;

-- ========================================
-- Monthly average close price per stock index
-- Purpose: Compare stock index performance over time
-- ========================================
SELECT 
    DATE_TRUNC('month', "Date") AS month,
    "Stock Index",
    ROUND(AVG("Close Price")::NUMERIC, 2) AS avg_close_price
FROM finance_dataset_clean
GROUP BY DATE_TRUNC('month', "Date"), "Stock Index"
ORDER BY month, "Stock Index";

-- ========================================
-- Most volatile stock indexes by average range
-- Purpose: Rank stock indexes by daily trading range
-- ========================================
SELECT 
    "Stock Index", 
    ROUND(AVG("Daily High" - "Daily Low")::NUMERIC, 2) AS avg_volatility
FROM finance_dataset_clean
GROUP BY "Stock Index"
ORDER BY avg_volatility DESC;

-- ========================================
-- Correlation between macroeconomic indicators and stock prices
-- Purpose: Evaluate how economic signals align with market prices
-- ========================================
SELECT 
    CORR("GDP Growth (%)"::NUMERIC, "Close Price"::NUMERIC) AS gdp_close_corr,
    CORR("Unemployment Rate (%)"::NUMERIC, "Close Price"::NUMERIC) AS unemp_close_corr,
    CORR("Inflation Rate (%)"::NUMERIC, "Close Price"::NUMERIC) AS infl_close_corr
FROM finance_dataset_clean;

-- ========================================
-- Highest monthly volatility by average daily range
-- Purpose: Determine the most volatile months overall
-- ========================================
SELECT 
    DATE_TRUNC('month', "Date") AS month,
    ROUND(AVG(("Daily High" - "Daily Low")::NUMERIC), 2) AS avg_volatility
FROM finance_dataset_clean
GROUP BY DATE_TRUNC('month', "Date")
ORDER BY avg_volatility DESC
LIMIT 5;

-- ========================================
-- Average close price by stock index
-- Purpose: Compare overall price levels across indexes
-- ========================================
SELECT 
    "Stock Index",
    ROUND(AVG("Close Price")::NUMERIC, 2) AS avg_close_price
FROM finance_dataset_clean
GROUP BY "Stock Index"
ORDER BY avg_close_price DESC;

-- ========================================
-- Largest single-day price drops
-- Purpose: Identify days with major intraday losses
-- ========================================
SELECT 
    "Date",
    "Stock Index",
    "Open Price",
    "Close Price",
    ROUND(("Open Price" - "Close Price")::NUMERIC, 2) AS drop_amount
FROM finance_dataset_clean
WHERE ("Open Price" - "Close Price") > 20
ORDER BY drop_amount DESC;
