def run_scd_type2_merge():
    """Executes the SCD Type 2 tracking logic using clean, isolated SQL commands."""
    print("🧠 Orchestrating SCD Type 2 historical compilation logic...")
    
    # Query 1: Close out modified historical records by setting end_date and is_current = False
    close_records_query = f"""
    UPDATE `{PROD_TABLE}` AS prod
    SET prod.end_date = staging.row_last_updated,
        prod.is_current = FALSE
    FROM `{STAGING_TABLE}` AS staging
    WHERE prod.customer_id = staging.customer_id
      AND prod.is_current = TRUE
      AND (prod.email != staging.email OR prod.subscription_tier != staging.subscription_tier);
    """
    
    # Query 2: Insert completely brand-new rows and the fresh versions of updated profiles
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
    WHERE prod.customer_id IS NULL; -- Ensures we only insert if no active match exists right now
    """
    
    print("  👉 Running step 1: Closing altered historical rows...")
    query_job1 = bq_client.query(close_records_query)
    query_job1.result()
    
    print("  👉 Running step 2: Appending new active records...")
    query_job2 = bq_client.query(insert_records_query)
    query_job2.result()
    
    print("🚀 SCD Type 2 verification process complete. Production table updated successfully.")