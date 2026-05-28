import os
import sys
from google.cloud import storage
from google.cloud import bigquery

# 1. Initialize Google Cloud Clients
project_id = "bqdemo-496217"  # Your exact project ID from your screenshot
storage_client = storage.Client(project=project_id)
bq_client = bigquery.Client(project=project_id)

# 2. Define Configurations
BUCKET_NAME = "gajay-customer-pipeline-data"
STAGING_TABLE = "bq_staging.customers_staging"
PROD_TABLE = "bq_production.customers_historical"

def load_csv_to_staging(batch_filename):
    """Wipes staging and loads a specific CSV file from GCS into BigQuery Staging."""
    print(f"🔄 Starting ingestion for {batch_filename}...")
    
    # Configure load job options to overwrite staging
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header row
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Overwrites old staging data
        autodetect=False,
    )
    
    gcs_uri = f"gs://{BUCKET_NAME}/landing_zone/{batch_filename}"
    
    print(f"📥 Loading data from {gcs_uri} into {STAGING_TABLE}...")
    load_job = bq_client.load_table_from_uri(
        gcs_uri, STAGING_TABLE, job_config=job_config
    )
    load_job.result()  # Wait for the job to finish
    print(f"✅ Staging table successfully populated with {batch_filename} details.")

def run_scd_type2_merge():
    """Executes the master SCD Type 2 tracking logic using a SQL MERGE statement."""
    print("🧠 Orchestrating SCD Type 2 historical compilation logic...")
    
    # This unified command handles updates, inserts, and history closure all in one atomic step
    merge_query = f"""
    MERGE `{PROD_TABLE}` AS prod
    USING (
      -- Select new records coming in from staging
      SELECT customer_id, name, email, subscription_tier, row_last_updated
      FROM `{STAGING_TABLE}`
      
      UNION ALL
      
      -- Force generation of tracking rows to close out modified profiles
      SELECT staging.customer_id, staging.name, staging.email, staging.subscription_tier, staging.row_last_updated
      FROM `{STAGING_TABLE}` AS staging
      JOIN `{PROD_TABLE}` AS historical
        ON staging.customer_id = historical.customer_id
       AND historical.is_current = TRUE
       AND (staging.email != historical.email OR staging.subscription_tier != historical.subscription_tier)
    ) AS src
    ON prod.customer_id = src.customer_id
    WHEN MATCHED AND prod.is_current = TRUE AND (prod.email != src.email OR prod.subscription_tier != src.subscription_tier) THEN
      UPDATE SET prod.end_date = src.row_last_updated, prod.is_current = FALSE
    WHEN NOT MATCHED THEN
      INSERT (customer_id, name, email, subscription_tier, row_last_updated, start_date, end_date, is_current)
      VALUES (src.customer_id, src.name, src.email, src.subscription_tier, src.row_last_updated, src.row_last_updated, TIMESTAMP('9999-12-31 23:59:59'), TRUE);
    """
    
    query_job = bq_client.query(merge_query)
    query_job.result()  # Wait for query to complete
    print("🚀 SCD Type 2 verification process complete. Production table updated successfully.")

if __name__ == "__main__":
    # Standard engineering practice: pass the filename dynamically via command arguments
    if len(sys.argv) < 2:
        print("❌ Error: Missing filename argument. Usage: python process_pipeline.py <filename.csv>")
        sys.exit(1)
        
    target_file = sys.argv[1]
    load_csv_to_staging(target_file)
    run_scd_type2_merge()