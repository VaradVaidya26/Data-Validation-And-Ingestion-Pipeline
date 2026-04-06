def get_config(dbutils):
    config = {}

    config["db_server"] = "datavalidation-dbs"
    config["db_port"] = "1433"
    config["db_name"] = "datavalidation_db"
    config["db_user"] = "Vaidya"

    config["jdbc_url"] = f"jdbc:sqlserver://{config['db_server']}:{config['db_port']};database={config['db_name']}"

    config["db_properties"] = {
        "user": config["db_user"],
        "password": dbutils.secrets.get(scope="datavalidationscope", key="sqldbpasswordkey"),
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    config["schema_table"] = "schema_metadata"
    config["date_columns"] = ["date_column"]   # update as needed
    config["date_format"] = "yyyy-MM-dd"

    config["landing_path"] = "/mnt/landing/"
    config["staging_path"] = "/mnt/staging/"
    config["rejected_path"] = "/mnt/rejected/"
    config["delta_path"] = "/mnt/delta/output/"

    return config