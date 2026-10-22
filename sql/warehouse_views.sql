-- DuckDB Data Warehouse Views (Phase 10)

-- 1. View gold_qoe_5m keeping latest update per grain, dimkey, window_end
CREATE OR REPLACE VIEW gold_qoe_5m AS
WITH ranked AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY grain, dimkey, window_end 
               ORDER BY updated_at DESC
           ) as rn
    FROM read_parquet('data/warehouse/gold_qoe_5m_raw/**/*.parquet')
)
SELECT * EXCLUDE (rn)
FROM ranked
WHERE rn = 1;

-- 2. View gold_network_5m keeping latest update
CREATE OR REPLACE VIEW gold_network_5m AS
WITH ranked AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY grain, dimkey, window_end 
               ORDER BY updated_at DESC
           ) as rn
    FROM read_parquet('data/warehouse/gold_network_5m_raw/**/*.parquet')
)
SELECT * EXCLUDE (rn)
FROM ranked
WHERE rn = 1;

-- 3. View silver_playback
CREATE OR REPLACE VIEW silver_playback AS
SELECT * FROM read_parquet('data/warehouse/silver_playback_events/**/*.parquet');

-- 4. View session_facts
CREATE OR REPLACE VIEW session_facts AS
SELECT * FROM read_parquet('data/warehouse/session_facts/**/*.parquet');

-- 5. View alerts
CREATE OR REPLACE VIEW alerts AS
SELECT * FROM read_parquet('data/warehouse/alerts/**/*.parquet');

-- 6. View ml_predictions
CREATE OR REPLACE VIEW ml_predictions AS
SELECT * FROM read_parquet('data/warehouse/ml_predictions/**/*.parquet');
