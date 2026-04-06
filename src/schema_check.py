def get_schema_from_sql(spark, jdbc_url, table_name, properties):
    return spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)


def validate_schema(df, schema_df):
    expected_columns = [row['column_name'] for row in schema_df.collect()]
    incoming_columns = df.columns

    if expected_columns != incoming_columns:
        return True, "Schema mismatch. Rule Failed"
    
    return False, ""