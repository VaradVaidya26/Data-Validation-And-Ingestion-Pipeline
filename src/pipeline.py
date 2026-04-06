from ingestion import get_file_path, read_csv
from validation import check_duplicates, validate_date_format
from schema_check import get_schema_from_sql, validate_schema
from utils import move_file, log


def run_pipeline(spark, dbutils, file_name, config):

    file_path = get_file_path(file_name, config["landing_path"])
    
    # Step 1: Read file
    df = read_csv(spark, file_path)
    log("File loaded successfully")

    # Step 2: Duplicate Check
    error_flag, error_message = check_duplicates(df)
    if error_flag:
        log(error_message)
        move_file(dbutils, file_path, config["rejected_path"] + file_name)
        return

    # Step 3: Schema Validation (from Azure SQL)
    schema_df = get_schema_from_sql(
        spark,
        config["jdbc_url"],
        config["schema_table"],
        config["db_properties"]
    )

    error_flag, error_message = validate_schema(df, schema_df)
    if error_flag:
        log(error_message)
        move_file(dbutils, file_path, config["rejected_path"] + file_name)
        return

    # Step 4: Date Validation
    error_flag, error_message = validate_date_format(
        df,
        config["date_columns"],
        config["date_format"]
    )

    if error_flag:
        log(error_message)
        move_file(dbutils, file_path, config["rejected_path"] + file_name)
        return

    # Step 5: Write to Delta
    df.write.format("delta").mode("append").save(config["delta_path"])
    log("Data written to Delta")

    # Step 6: Move to staging
    move_file(dbutils, file_path, config["staging_path"] + file_name)
    log("File moved to staging")