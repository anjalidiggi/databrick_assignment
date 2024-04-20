from pyspark.sql.types import StructType, StringType, StructField, IntegerType
from pyspark.sql.functions import current_date


def read_csv_data(path):
    df = spark.read.csv(path, header=True)
    return df



def write_csv_file(df, path):
    df.write.format("csv").save(path)



def read_schema(path, schema):
    df = spark.read.csv(path=path, header="false", schema=schema)
    return df



def read_schema_options(path, schema):
    df = spark.read.format("csv").options(header=False).schema(schema).load(path)
    return df



def change_column_case_to_snake_case(df):
    def camel_to_snake_case(column_name):
        return ''.join(['_' + c.lower() if c.isupper() else c for c in column_name]).lstrip('_')

    for column in df.columns:
        new_column_name = camel_to_snake_case(column)
        df = df.withColumnRenamed(column, new_column_name)

    return df



def write_delta_table(df, database, table, primary_key, path):
    df.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("path", path) \
        .saveAsTable(f"{database}.{table}")

    return df



def read_with_custom_schema(data, schema):
    df = spark.read.csv(data, schema)
    return df



def read_with_custom_schema_format(data, schema):
    df = spark.read.format('csv').schema(schema).load(data)
    return df



def add_current_date(df):
    df = df.withColumn("load_date", current_date())
    return df

