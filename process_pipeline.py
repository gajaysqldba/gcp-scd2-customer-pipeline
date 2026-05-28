import os
import sys
from google.cloud import storage
from google.cloud import bigquery

# =====================================================================
# 1. INITIALIZE GOOGLE CLOUD CLIENTS
# =====================================================================
# Hardcoded to your exact project configuration environment
project_id = "bqdemo-496217"  
storage_client = storage.Client(project=project_id)
bq_client = bigquery.Client(project=project_id)

# =====================================================================
# 2. DEFINE ENVIRONMENT CONFIGURATIONS
# =====================================================================
BUCKET_NAME = "gajay-customer-pipeline-data"
STAGING_TABLE = "bq_staging.customers_staging"
PROD_TABLE = "bq_production.customers_historical"

def load_csv_to_staging(batch_filename):
    """
    Wipes the staging table clean (WRITE_TRUNCATE) and streams a specific 
    raw CSV batch file from Google Cloud Storage into BigQuery Staging.
    """
    print(f"🔄 Starting ingestion sequence for file: {batch_filename}...")
    
    # Configure load job options to overwrite staging data each run
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,                         # Skip the CSV header row
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, 
        autodetect=False,
    )
    
    gcs_uri = f"gs://{BUCKET_NAME}/landing_zone/{batch_filename}"
    
    print(f"📥 Extracting from Data Lake: {gcs_uri} -> Loading to: {STAGING_TABLE}...")
    load_job = bq_client.load_table_from_uri(
        gcs_uri, STAGING_TABLE, job_config=job_config
    )
    load_job.result()  # Blocks execution until the load job completes successfully
    print(f"✅ Staging table successfully populated with {batch_filename} data structure.")


def run_scd_type2_merge():
    """
    Executes the Slowly Changing Dimension (SCD Type 2) tracking sequence.
    Separated into two distinct, isolated transactions to satisfy BigQuery 
    row-matching rules.
    """
    print("🧠 Orchestrating SCD Type 2 historical compilation logic...")
    
    # QUERY 1: Look for records where profile values changed. Capping old records.
    close_records_query = f"""
    UPDATE `{PROD_TABLE}` AS prod
    SET prod.end_date = staging.row_last_updated,
        prod.is_current = FALSE
    FROM `{STAGING_TABLE}` AS staging
    WHERE prod.customer_id = staging.customer_id
      AND prod.is_current = TRUE
      AND (prod.email != staging.email OR prod.subscription_tier != staging.subscription_tier);
    """
    
    # QUERY 2: Append brand-new IDs OR the fresh version records of updated IDs
    insert_records_query = f"""
    INSERT INTO `{PROD_TABLE}` (customer_id, name, email, subscription_tier, row_last_updated, start_date, end_date, is_current)
    SELECT 
        staging.customer_id, 
        staging.name, 
        staging.email, 
        staging.subscription_tier, 
        staging.row_last_updated,
        staging.row_last_updated AS start_date,
        TIMESTAMP('9999-12-31 23:59:59') AS end_date,
        TRUE AS is_current
    FROM `{STAGING_TABLE}` AS staging
    LEFT JOIN `{PROD_TABLE}` AS prod
      ON staging.customer_id = prod.customer_id
     AND prod.is_current = TRUE
     AND prod.email = staging.email
     AND prod.subscription_tier = staging.subscription_tier
    WHERE prod.customer_id IS NULL;
    """
    
    print("  👉 Running Phase 1: Retiring altered active rows (SCD2 Close)...")
    query_job1 = bq_client.query(close_records_query)
    query_job1.result()  # Wait for update query to complete
    
    print("  👉 Running Phase 2: Appending new tracking increments (SCD2 Open)...")
    query_job2 = bq_client.query(insert_records_query)
    query_job2.result()  # Wait for insert query to complete
    
    print("🚀 SCD Type 2 verification process complete. Production master history table updated.")


# =====================================================================
# 3. RUNNER EXECUTION ENTRYPOINT
# =====================================================================
if __name__ == "__main__":
    # Ensure a target batch filename argument is passed from Cloud Build execution context
    if len(sys.argv) < 2:
        print("❌ Error: Missing target batch filename. Usage: python process_pipeline.py <filename.csv>")
        sys.exit