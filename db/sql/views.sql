-- View for direct urban-rural temperature comparison (hourly data)
CREATE MATERIALIZED VIEW IF NOT EXISTS urban_rural_hourly AS
SELECT 
    u.timestamp,
    ul.name AS urban_location,
    u.temperature AS urban_temp,
    r.temperature AS rural_temp,
    u.temperature - r.temperature AS temp_differential,
    ul.location_id AS urban_id,
    rl.location_id AS rural_id
FROM temperature_data u
JOIN locations ul ON u.location_id = ul.location_id
JOIN locations rl ON ul.location_id = rl.urban_pair_id
JOIN temperature_data r ON r.location_id = rl.location_id AND r.timestamp = u.timestamp
WHERE ul.is_urban = TRUE;

-- Daily aggregation view
CREATE MATERIALIZED VIEW IF NOT EXISTS urban_rural_daily AS
SELECT 
    date_trunc('day', timestamp) AS date,
    urban_location,
    AVG(urban_temp) AS avg_urban_temp,
    AVG(rural_temp) AS avg_rural_temp,
    AVG(temp_differential) AS avg_differential,
    MAX(temp_differential) AS max_differential,
    MIN(temp_differential) AS min_differential
FROM urban_rural_hourly
GROUP BY date_trunc('day', timestamp), urban_location
ORDER BY date_trunc('day', timestamp), urban_location;

CREATE MATERIALIZED VIEW IF NOT EXISTS heat_island_intensity_daily AS
WITH rural_daily AS (
    -- First aggregate rural temperatures to daily level
    SELECT 
        rl.urban_pair_id,
        date_trunc('day', r.timestamp) AS date,
        AVG(r.temperature) AS avg_temp,
        STDDEV(r.temperature) AS daily_std
    FROM temperature_data r
    JOIN locations rl ON r.location_id = rl.location_id
    WHERE rl.is_urban = FALSE
    GROUP BY rl.urban_pair_id, date_trunc('day', r.timestamp)
),
rural_stats AS (
    -- Then calculate rolling statistics on the daily data
    SELECT 
        urban_pair_id,
        date,
        AVG(daily_std) OVER (
            PARTITION BY urban_pair_id 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS rolling_std
    FROM rural_daily
),
daily_diff_from_hourly AS (
    -- Use the existing hourly view and aggregate to daily
    SELECT 
        date_trunc('day', timestamp) AS date,
        urban_id,
        urban_location,
        AVG(temp_differential) AS avg_temp_diff
    FROM urban_rural_hourly
    GROUP BY date_trunc('day', timestamp), urban_id, urban_location
)
SELECT 
    d.date,
    d.urban_location,
    d.avg_temp_diff,
    rs.rolling_std,
    d.avg_temp_diff / NULLIF(rs.rolling_std, 0) AS heat_island_intensity
FROM daily_diff_from_hourly d
JOIN rural_stats rs ON d.urban_id = rs.urban_pair_id AND d.date = rs.date
ORDER BY d.date, d.urban_location;

-- Hourly Heat Island Intensity view, keep for later, possible use for future hourly analysis
CREATE MATERIALIZED VIEW IF NOT EXISTS heat_island_intensity_hourly AS
WITH rural_hourly_stats AS (
    SELECT 
        rl.urban_pair_id,
        r.timestamp,
        r.temperature,
        STDDEV(r.temperature) OVER (
            PARTITION BY rl.urban_pair_id 
            ORDER BY r.timestamp 
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS rolling_std_24h
    FROM temperature_data r
    JOIN locations rl ON r.location_id = rl.location_id
    WHERE rl.is_urban = FALSE
)
SELECT 
    u.timestamp,
    ul.name AS urban_location,
    u.temperature AS urban_temp,
    r.temperature AS rural_temp,
    (u.temperature - r.temperature) AS temp_differential,
    r.rolling_std_24h,
    (u.temperature - r.temperature) / NULLIF(r.rolling_std_24h, 0) AS hourly_heat_island_intensity
FROM temperature_data u
JOIN locations ul ON u.location_id = ul.location_id
JOIN locations rl ON ul.location_id = rl.urban_pair_id
JOIN rural_hourly_stats r ON r.urban_pair_id = rl.urban_pair_id 
                          AND r.timestamp = u.timestamp
WHERE ul.is_urban = TRUE
ORDER BY u.timestamp, ul.name;

-- For compatibility with existing code, create an alias view
CREATE MATERIALIZED VIEW IF NOT EXISTS heat_island_intensity AS
SELECT * FROM heat_island_intensity_daily;

-- Time-of-day analysis view, keep for later, possible use for future hourly analysis
CREATE MATERIALIZED VIEW IF NOT EXISTS time_of_day_pattern AS
SELECT 
    extract(hour from timestamp) AS hour_of_day,
    urban_location,
    AVG(temp_differential) AS avg_differential,
    STDDEV(temp_differential) AS std_differential
FROM urban_rural_hourly
GROUP BY extract(hour from timestamp), urban_location
ORDER BY urban_location, hour_of_day;