#Fetch the data from the given API by passing the parameter as a page and retrieving the data till the data is empty
import requests

url = 'https://reqres.in/api/users'
data = []
page = 1

while True:
    response = requests.get(url, params={'page': page})
    json_data = response.json()
    if not json_data['data']:
        break
    data.extend(json_data['data'])
    page += 1

#Read the data frame with a custom schema
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('email', StringType(), True),
    StructField('first_name', StringType(), True),
    StructField('last_name', StringType(), True),
    StructField('avatar', StringType(), True)
])

df = spark.createDataFrame(data, schema)

#Flatten the dataframe
from pyspark.sql.functions import col

df_flat = df.select(
    col('id'),
    col('email'),
    col('first_name'),
    col('last_name'),
    col('avatar')
)

#Derive a new column from email as site_address with values(reqres.in)
from pyspark.sql.functions import split, col

df_final = df_flat.withColumn(
    'site_address',
    split(col('email'), '@')[1]
)

#Add load_date with the current date
from pyspark.sql.functions import current_date

df_final = df_final.withColumn(
    'load_date',
    current_date()
)

#Write the data frame to location in DBFS as /db_name /table_name with Db_name as site_info and table_name as person_info with delta format and overwrite mode.

df_final.write.format('delta').mode('overwrite').save('dbfs:/FileStore/assignment/question2/site_info/person_info')

testing_df = spark.read.format('delta').load('dbfs:/FileStore/assignment/question2/site_info/person_info')

