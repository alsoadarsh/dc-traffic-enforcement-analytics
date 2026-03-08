-- ============================================================
-- DC Traffic Enforcement Analytics
-- Operational Analysis
-- Author: Adarsh Shukla
-- ============================================================


-- ============================================================
-- AGENCY PERFORMANCE
-- ============================================================

-- Monthly enforcement activity by agency
-- Tracks ticket volume trends over time
-- to surface seasonal patterns and workload distribution
SELECT
    YEAR(issue_date)            AS year,
    MONTH(issue_date)           AS month,
    issuing_agency_name,
    COUNT(*)                    AS total_tickets
FROM moving_violations
GROUP BY year, month, issuing_agency_name
ORDER BY year, month, issuing_agency_name;


-- Recent enforcement activity since October 2024
-- Ranks agencies by ticket volume
-- in the most recent enforcement period
SELECT
    issuing_agency_name,
    COUNT(*)                    AS total_tickets
FROM moving_violations
WHERE issue_date >= '2024-10-01'
GROUP BY issuing_agency_name
ORDER BY total_tickets DESC;


-- ============================================================
-- TEMPORAL PATTERNS
-- ============================================================

-- Average daily ticket volume by day of week
-- Identifies peak enforcement days
-- and weekly violation cycles
SELECT
    day_of_week,
    ROUND(AVG(ticket_count), 0) AS avg_tickets_per_day,
    day_num
FROM (
    SELECT
        DAYNAME(issue_date)     AS day_of_week,
        DAYOFWEEK(issue_date)   AS day_num,
        COUNT(*)                AS ticket_count
    FROM moving_violations
    GROUP BY issue_date
) AS daily_counts
GROUP BY day_of_week, day_num
ORDER BY day_num;


-- Average ticket volume by hour of day
-- Maps enforcement concentration
-- across a standard 24-hour operating window
SELECT
    hour_of_day,
    ROUND(AVG(ticket_count), 0) AS avg_tickets_per_hour
FROM (
    SELECT
        HOUR(issue_time)        AS hour_of_day,
        COUNT(*)                AS ticket_count
    FROM moving_violations
    WHERE issue_time IS NOT NULL
    GROUP BY HOUR(issue_time)
) AS hourly_counts
GROUP BY hour_of_day
ORDER BY hour_of_day;


-- ============================================================
-- WEATHER IMPACT ANALYSIS
-- ============================================================

-- Violation volume during active precipitation
-- Cross-references violations with weather readings
-- to assess whether rain deters driving behavior
SELECT
    COUNT(*)                    AS tickets_during_rain
FROM moving_violations mv
JOIN weather_dc w
    ON DATE(mv.issue_date) = DATE(w.date_time)
WHERE w.rain > 0;


-- Monthly precipitation totals
-- Establishes weather baseline for
-- contextualizing violation volume trends
SELECT
    YEAR(date_time)             AS year,
    MONTH(date_time)            AS month,
    ROUND(SUM(rain), 2)         AS total_precipitation_mm
FROM weather_dc
GROUP BY YEAR(date_time), MONTH(date_time)
ORDER BY year, month;


-- Accident-related violations by weather condition
-- Compares accident ticket rates on rainy
-- vs non-rainy days to quantify weather impact on safety
SELECT
    CASE
        WHEN w.rain > 0 THEN 'Rainy'
        ELSE 'Non-Rainy'
    END                         AS weather_condition,
    COUNT(*)                    AS accident_tickets
FROM moving_violations mv
JOIN weather_dc w
    ON DATE(mv.issue_date) = DATE(w.date_time)
WHERE mv.accident = 1
GROUP BY weather_condition
ORDER BY weather_condition;


-- ============================================================
-- FINANCIAL ANALYSIS
-- ============================================================

-- Monthly fine revenue from high-severity speeding
-- Isolates violations exceeding 10 mph over the limit
-- to track enforcement trends and revenue concentration
SELECT
    YEAR(issue_date)            AS year,
    MONTH(issue_date)           AS month,
    SUM(fine_amount)            AS total_fines,
    GROUP_CONCAT(
        DISTINCT violation_desc
        ORDER BY violation_desc
        SEPARATOR '; '
    )                           AS violation_types
FROM moving_violations
WHERE
    (violation_desc LIKE '%SPEED%'
    AND violation_desc LIKE '%MPH%')
    AND violation_desc NOT LIKE '%UP TO TEN MPH%'
    AND violation_desc NOT LIKE '%10 MPH%'
GROUP BY year, month
ORDER BY year, month;
