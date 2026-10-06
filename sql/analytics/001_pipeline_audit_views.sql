-- =====================================================================
-- Operational Analytics & Pipeline Audit Views
-- Compatible with SQLite (pipeline_state.db) and PostgreSQL
-- =====================================================================

-- 1. Pipeline Execution Summary by Pipeline & Dataset
CREATE VIEW IF NOT EXISTS vw_pipeline_run_summary AS
SELECT
    pipeline_name,
    source_system,
    dataset,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successful_runs,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_runs,
    SUM(records_extracted) AS total_records_extracted,
    SUM(records_processed) AS total_records_processed,
    SUM(records_rejected) AS total_records_rejected,
    MAX(started_at) AS last_run_at
FROM pipeline_runs
GROUP BY pipeline_name, source_system, dataset;

-- 2. Audit View for Failed Pipeline Runs
CREATE VIEW IF NOT EXISTS vw_pipeline_failed_runs AS
SELECT
    run_id,
    pipeline_name,
    source_system,
    dataset,
    batch_id,
    started_at,
    finished_at,
    error_type,
    error_message
FROM pipeline_runs
WHERE status = 'failed'
ORDER BY started_at DESC;

-- 3. Latest Incremental Dataset Watermarks
CREATE VIEW IF NOT EXISTS vw_dataset_ingestion_watermarks AS
SELECT
    w.source_system,
    w.dataset,
    w.watermark,
    w.updated_at,
    w.run_id
FROM watermarks w;

