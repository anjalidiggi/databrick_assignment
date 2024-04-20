#%run "/Users/anjali.gupta@diggibyte.com/databricksassignment/assignment1/source_to_bronze/utils"

from pyspark.sql.types import StructField,StructType,StringType,IntegerType


employee_custom_schema = StructType([
    StructField('EmployeeID', IntegerType(), True),
    StructField('EmployeeName', StringType(), True),
    StructField('Department', StringType(), True),
    StructField('Country', StringType(), True),
    StructField('Salary', IntegerType(), True),
    StructField('Age', IntegerType(), True)
])


country_custom_schema = StructType([
    StructField('CountryCode', StringType(), True),
    StructField('CountryName', StringType(), True)
])



department_custom_schema = StructType([
    StructField('DepartmentID', StringType(), True),
    StructField('DepartmentName', StringType(), True)
])


employee_csv_path = "dbfs:/FileStore/source_to_bronze/Employee_Q1.csv"
country_csv_path = "dbfs:/FileStore/source_to_bronze/Country_Q1.csv"
department_csv_path = "dbfs:/FileStore/source_to_bronze/Department_Q1.csv"





employee_df =read_with_custom_schema(employee_csv_path,employee_custom_schema)

country_df = read_with_custom_schema_format(country_csv_path, country_custom_schema)

department_df = read_with_custom_schema(department_csv_path, department_custom_schema)



display(employee_df)
display(country_df)
display(department_df)


employee_snake_case_df = change_column_case_to_snake_case(employee_df)
display(employee_snake_case_df)



department_snake_case_df = change_column_case_to_snake_case(department_df)
display(department_snake_case_df)



country_snake_case_df = change_column_case_to_snake_case(country_df)
display(country_snake_case_df)



employee_with_date_df = add_current_date(employee_snake_case_df)
display(employee_with_date_df)


department_with_date_df = add_current_date(department_snake_case_df)
display(department_with_date_df)


country_with_date_df = add_current_date(country_snake_case_df)
display(country_with_date_df)


spark.sql('create database employee_info')
spark.sql('use employee_info')

employee_df.write.option('path', 'dbfs:/FileStore/silver/employee_info/dim_employees').saveAsTable('dim_employees')




