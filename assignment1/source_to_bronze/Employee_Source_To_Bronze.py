
#%run "/Users/anjali.gupta@diggibyte.com/databricksassignment/assignment1/source_to_bronze/utils"

dbutils.widgets.text("Employee-Q1", "", "Employee")
dbutils.widgets.text("Department-Q1", "", "Department")
dbutils.widgets.text("Country-Q1", "", "Country")
emp = dbutils.widgets.get("Employee-Q1")
dep = dbutils.widgets.get("Department-Q1")
coun = dbutils.widgets.get("Country-Q1")

emp_source_path = f"dbfs:/FileStore/resource/{emp}.csv"
emp_bronze_path = f"dbfs:/FileStore/source_to_bronze/{emp}.csv"
emp_reading_csv_file = read_csv_data(emp_source_path)
emp_reading_csv_file.display()
write_csv_file(emp_reading_csv_file, emp_bronze_path)

dep_source_path = f"dbfs:/FileStore/resource/{dep}.csv"
dep_bronze_path = f"dbfs:/FileStore/source_to_bronze/{dep}.csv"
dep_reading_csv_file = read_csv_data(dep_source_path)
dep_reading_csv_file.display()
write_csv_file(dep_reading_csv_file, dep_bronze_path)

coun_source_path = f"dbfs:/FileStore/resource/{coun}.csv"
coun_bronze_path = f"dbfs:/FileStore/source_to_bronze/{coun}.csv"
coun_reading_csv_file = read_csv_data(coun_source_path)
coun_reading_csv_file.display()
write_csv_file(coun_reading_csv_file, coun_bronze_path)
