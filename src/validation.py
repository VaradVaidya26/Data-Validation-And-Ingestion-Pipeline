import pyspark.sql.functions as F

def check_duplicates(df):
    total_count = df.count()
    distinct_count = df.distinct().count()

    if distinct_count != total_count:
        return True, "Duplication Found. Rule 1 Failed"
    
    return False, ""


def validate_date_format(df, date_columns, expected_format):
    for col_name in date_columns:
        parsed_col = F.to_date(F.col(col_name), expected_format)
        
        invalid_count = df.filter(parsed_col.isNull()).count()
        
        if invalid_count > 0:
            return True, f"Invalid date format in column {col_name}"
    
    return False, ""