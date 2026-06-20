# Databricks notebook source
type('abc')#----to check the data type 

# COMMAND ----------

type(spark) #----to check the spark session name

# COMMAND ----------

data=[(1,'sarath',5000),(2,'sai',50000),(3,'kunapareddy',56000)]

schema=['id','name','salary']

df=spark.createDataFrame(data=data,schema=schema)    #----to create a dataframe
df.show() #-----to see the data 
df.printSchema() #-----to see the schema
df.count() #---to see the count of rows

# COMMAND ----------

data=[(1,'sarath',5000),(2,'sai',50000),(3,'kunapareddy',56000)]

schema="id int,name string,salary integer" #--------we can declare the schema in many ways

df=spark.createDataFrame(data=data,schema=schema)    #----to create a dataframe
df.show() #-----to see the data 
df.printSchema() #-----to see the schema
df.count() #---to see the count of rows

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType 

data=[(1,'sarath',5000),(2,'sai',50000),(3,'kunapareddy',56000)]

#--------we can declare the schema in many ways ---this way is the best way
schema=StructType([
    StructField('id',IntegerType(),True),
    StructField('name',StringType(),True),
    StructField('salary',IntegerType()  ,True)
])


df=spark.createDataFrame(data=data,schema=schema)    #----to create a dataframe
df.show() #-----to see the data 
df.printSchema() #-----to see the schema
df.count() #---to see the count of rows

# COMMAND ----------

data=[
    {'id':1,'name':'sarath','sal':400000},
    {'id':2,'name':'sai','sal':500000},
    {'id':3,'name':'kunapareddy','sal':600000}
]
#-----since in json we have the schema information so we no need to define the schema explicty , but still we can define aswell


df=spark.createDataFrame(data=data)    #----to create a dataframe
df.show() #-----to see the data 
df.printSchema() #-----to see the schema
df.count() #---to see the count of rows

# COMMAND ----------

data=[
    {'id':1,'name':'sarath','sal':400000},
    {'id':2,'name':'sai','sal':500000},
    {'id':3,'name':'kunapareddy','sal':600000}
]
#-----since in json we have the schema information so we no need to define the schema explicty , but still we can define aswell

df=spark.createDataFrame(data=data,schema='id int,name string')    #----to create a dataframe, here we required only two columns 
#------------------------------------------------------------------even though we have three columns in the data we can define only two columns

df.show() #-----to see the data 
df.printSchema() #-----to see the schema
df.count() #---to see the count of rows

# COMMAND ----------

#----to read the data from the csv files we use like this 

path='abfss://datafiles@landingzoneblob.dfs.core.windows.net/landing/csv_files/FCT_SALES_2009.csv'

df=spark.read.format('csv').option('header','True').option('mode','permissive').option('inferSchema','True').load(path)
print(type(df ))
display(df)

# COMMAND ----------

#----to read the data from the csv files we use like this 

#----we have so many modes while reading the data from csv files like dropmalformed, failfast, permissive  
# -----dropmalformed --> if there is any malformed data in the csv file it will drop that row and continue with the other rows
# -----failfast --> if there is any malformed data in the csv file it will throw an error and stop the execution
# -----permissive --> if there is any malformed data in the csv file it will replace the malformed data with null and continue with the other rows 

path='abfss://datafiles@landingzoneblob.dfs.core.windows.net/landing/csv_files/FCT_SALES_2009.csv'

df=spark.read.format('csv').option('header','True').option('mode','dropmalformed').option('inferSchema','True').load(path)
print(type(df )) #-----to dispaly the the data type --> whether it is dataframe or not 
display(df)

# COMMAND ----------

#----------to write the data into csv file
df.write.format('csv').option('header','True').option('mode','overwrite').save('abfss://datafiles@landingzoneblob.dfs.core.windows.net/landing/csv_files/write_FCT_SALES_2009')


#------to write the data data into single file csv , instead of parallel files
df.coalesce(1).write.format('csv').option('header','True').option('mode','overwrite').save('abfss://datafiles@landingzoneblob.dfs.core.windows.net/landing/csv_files/s_write_FCT_SALES_2009')



# COMMAND ----------

#----to write the file into json file
df.write.format('json').mode('overwrite').save('abfss://datafiles@landingzoneblob.dfs.core.windows.net/landing/json_files/write_json_FCT_SALES_2009.json')

# COMMAND ----------

#----to read the data from json 

path='abfss://datafiles@landingzoneblob.dfs.core.windows.net/landing/json_files/write_json_FCT_SALES_2009.json'
df=spark.read.format('json').option('header','True').option('mode','permissive').load(path)
display(df)

df1=spark.read.format('json').option('header','True').option('mode','permissive').option('schema',"Date string,DateKey long,DiscountAmount double,").load(path)
display(df1)


# COMMAND ----------

#-----------to write the parquet file format
df.write.format('parquet').mode('overwrite').save('abfss://datafiles@landingzoneblob.dfs.core.windows.net/landing/parquet_files/write_parquet_FCT_SALES_2009.parquet')


# COMMAND ----------

df=spark.read.format('parquet').option('mode', 'PERMISSIVE').load('abfss://datafiles@landingzoneblob.dfs.core.windows.net/landing/parquet_files/write_parquet_FCT_SALES_2009.parquet')
display(df)

# COMMAND ----------

from datetime import datetime, timedelta

dept_data = [
    (10, 'ACCOUNTING', 'NEW YORK'),
    (20, 'RESEARCH', 'DALLAS'),
    (30, 'SALES', 'CHICAGO'),
    (40, 'OPERATIONS', 'BOSTON')
]
dept_schema = ['deptno', 'dname', 'loc']
dept = spark.createDataFrame(dept_data, dept_schema)

emp_data = [
    (7839, 'KING', 'PRESIDENT', None, datetime.strptime('17-11-1981', '%d-%m-%Y'), 5000, None, 10),
    (7698, 'BLAKE', 'MANAGER', 7839, datetime.strptime('1-5-1981', '%d-%m-%Y'), 2850, None, 30),
    (7782, 'CLARK', 'MANAGER', 7839, datetime.strptime('9-6-1981', '%d-%m-%Y'), 2450, None, 10),
    (7566, 'JONES', 'MANAGER', 7839, datetime.strptime('2-4-1981', '%d-%m-%Y'), 2975, None, 20),
    (7788, 'SCOTT', 'ANALYST', 7566, datetime.strptime('13-07-1987', '%d-%m-%Y') - timedelta(days=85), 3000, None, 20),
    (7902, 'FORD', 'ANALYST', 7566, datetime.strptime('3-12-1981', '%d-%m-%Y'), 3000, None, 20),
    (7369, 'SMITH', 'CLERK', 7902, datetime.strptime('17-12-1980', '%d-%m-%Y'), 800, None, 20),
    (7499, 'ALLEN', 'SALESMAN', 7698, datetime.strptime('20-2-1981', '%d-%m-%Y'), 1600, 300, 30),
    (7521, 'WARD', 'SALESMAN', 7698, datetime.strptime('22-2-1981', '%d-%m-%Y'), 1250, 500, 30),
    (7654, 'MARTIN', 'SALESMAN', 7698, datetime.strptime('28-9-1981', '%d-%m-%Y'), 1250, 1400, 30),
    (7844, 'TURNER', 'SALESMAN', 7698, datetime.strptime('8-9-1981', '%d-%m-%Y'), 1500, 0, 30),
    (7876, 'ADAMS', 'CLERK', 7788, datetime.strptime('13-07-1987', '%d-%m-%Y') - timedelta(days=51), 1100, None, 20),
    (7900, 'JAMES', 'CLERK', 7698, datetime.strptime('3-12-1981', '%d-%m-%Y'), 950, None, 30),
    (7934, 'MILLER', 'CLERK', 7782, datetime.strptime('23-1-1982', '%d-%m-%Y'), 1300, None, 10)
]
emp_schema = ['empno', 'ename', 'job', 'mgr', 'hiredate', 'sal', 'comm', 'deptno']
emp = spark.createDataFrame(emp_data, emp_schema)

# COMMAND ----------

display(emp)
display(dept)

# COMMAND ----------

#-to add the column or change in existing column
emp1=emp.withColumn('sal',emp['sal']+100)
display(emp1)

# COMMAND ----------

#-----if u are good in sql we can use like this withour creating the table 
#---emp is the dataframe 
spark.sql("select * from {table} ",table=emp).show()

# COMMAND ----------

#---------we can write the sql queries with all the synatx and all others necceasry things in the sql query

df1=spark.sql("""select e.*, dense_rank() over(partition by E.Deptno  order by Sal  Desc) as rnk from  {tbl1} as E,{tbl2} as  D 
              where  e.deptno=d.deptno
              """,tbl1=emp,tbl2=dept).show()

# COMMAND ----------


#-----we can use joins in sql querey by using in below method 
df1=spark.sql("""select e.*,d.* from  {tbl1} as E,{tbl2} as  D 
              where  e.deptno=d.deptno
              """,tbl1=emp,tbl2=dept)
display(df1)


# COMMAND ----------

emp_1= emp.withColumnRenamed('sal','salary')
display(emp_1)

# COMMAND ----------

#---for example if i want to multiple columnns 
""" 
sss={
    "rating_1": 100,
    "rating_2": 200,
    "rating_3": 300
}
"""

#emp_1= emp.withColumns(sss)
#display(emp_1)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data = [
    (1, ('sai', 'Sarath'), 3000),
    (2, ('jaffa', 'tappa'), 5000)
]

#---- if we observe the data the name is in different format so we will create a struct type for this complex datatype

structname = StructType([
    StructField('Firstname', StringType(), True),
    StructField('Lastname', StringType(), True)
])

schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('name', structname, True),
    StructField('salary', IntegerType(), True)
])

df = spark.createDataFrame(data, schema)

display(df)
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Array Type  columns


from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

data = [('abc', [1, 2]), ('mon', [3, 4]), ('xyz', [7, 8])]
schema = ['id', 'numbers']

df = spark.createDataFrame(data, schema)
display(df)
df.printSchema()

#—— —— —— —— —— please observe the output of these above one and datatypes of these columns

data = [('abc', [1, 2]), ('mon', [3, 4]), ('xyz', [7, 8])]
schema = StructType([
    StructField('id', StringType(), True),
    StructField('numbers', ArrayType(IntegerType()), True)
])

df = spark.createDataFrame(data, schema)
display(df)
df.printSchema()

# COMMAND ----------

df2 = df.withColumn('firstnumber', df.numbers[0])  #————————————————————————this is used to add a new column from existing column  with the index value of 0 so. The value will take first  value of that column

display(df2)

# COMMAND ----------

#----------------array is used to add the multiple columns to arraytype 
from pyspark.sql.functions import array, col

data = [(1, 2), (3, 4)]

schema = ['num1', 'num2']

df = spark.createDataFrame(data, schema)

df1 = df.withColumn('numbers', array(col('num1'), col('num2')))

display(df1)

# COMMAND ----------

#———————— these commands are mostly used. When we use array functions 

# explode()——> it is used to create a new row for each element in the given array column

from pyspark.sql.functions import explode

data = [
    (1, 'sarath', ['dotnet', 'azure']),
    (2, 'wafa', ['java', 'aws'])
]

schema = ['id', 'name', 'skills']

df = spark.createDataFrame(data, schema)

display(df)
df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import explode, col

data = [
    (1, 'sarath', ['dotnet', 'azure']),
    (2, 'wafa', ['java', 'aws'])
]

schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('skills', ArrayType(StringType()), True)
])

df = spark.createDataFrame(data, schema)

df_exploded = df.withColumn('explodedcol', explode(col('skills'))).select('id', 'explodedcol')

display(df_exploded)

# COMMAND ----------

# Split() :———split function returns the array type after splitting the string column by delimiter.

from pyspark.sql.functions import split, col

# Data and schema
data = [(1, 'sarath', 'dotnet,azure'), (2, 'wafa', 'java,aws')]
schema = ['id', 'name', 'skills']

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Display the DataFrame and its schema
display(df)
df.printSchema()

# Add a new column 'skill_array' by splitting the 'skills' column
df1 = df.withColumn('skill_array', split(col('skills'), ','))    #-----------------split column to array from string 

# Show the updated DataFrame
display(df1)

# COMMAND ----------

#------------------Create DataFrame with ArrayType column using StructType schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
data = [(1, 'sai', 'parchur', [1, 2]), (2, 'sarath', 'chicago', [3, 4]), (3, 'kunapareddy', 'dallas', [5, 6])]
schema = StructType([
    StructField('id', IntegerType()),
    StructField('name', StringType()),
    StructField('location', StringType()),
    StructField('skills', ArrayType(IntegerType()))
])
df = spark.createDataFrame(data, schema)
display(df)
df.printSchema()

#------------------Create DataFrame with simple schema and add array column from existing columns
from pyspark.sql.functions import col, array, explode
data = [(1, 'sai', 'parchur', [1, 2]), (2, 'sarath', 'chicago', [3, 4]), (3, 'kunapareddy', 'dallas', [5, 6])]
schema = ['id', 'name', 'location', 'skills']
df = spark.createDataFrame(data, schema)
display(df)

# Add new array column combining 'location' and 'name'
df1 = df.withColumn('new_array_column', array(col('location'), col('name')))
display(df1)

# Explode the new array column to create a row for each element
df2 = df1.withColumn('explodedcol', explode(col('new_array_column'))).select('id', 'name', 'location', 'skills', 'explodedcol')
display(df2)

#------------------Split string column to array and check for value in array
from pyspark.sql.functions import split, array_contains
data = [(1, 'sai', 'parchur', '1,2'), (2, 'sarath', 'chicago', '3,4'), (3, 'kunapareddy', 'dallas', '5,6')]
schema = ['id', 'name', 'location', 'skills']
df = spark.createDataFrame(data, schema)

# Split 'skills' string column into array
df3 = df.withColumn('newcolumn_array', split(col('skills'), ','))
display(df3)

# Check if '1' is present in the new array column
df4 = df3.withColumn('ssss', array_contains(col('newcolumn_array'), '1'))
display(df4)

# COMMAND ----------

# Array Functions Reference Table with Examples

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.sql.functions import array, explode, split, array_contains, array_append, array_compact, array_distinct, array_except, array_intersect, array_join, array_max, array_min, array_position, array_remove, lit

# Sample DataFrame with array column
data = [
    (1, 'sai', 'parchur', [1, 2, 2, None]),
    (2, 'sarath', 'chicago', [3, 4]),
    (3, 'kunapareddy', 'dallas', [5, 6])
]
schema = StructType([
    StructField('id', IntegerType()),
    StructField('name', StringType()),
    StructField('location', StringType()),
    StructField('skills', ArrayType(IntegerType()))
])
df = spark.createDataFrame(data, schema)
display(df)

# 1. ArrayType(): Already used in 'skills' column above

# 2. array(): Combine columns into array
df_array = df.withColumn("name_loc", array("name", "location"))
display(df_array)

# 3. explode(): Flatten array into rows
df_explode = df.select("id", explode("skills").alias("skill"))
display(df_explode)

# 4. split(): Split string into array
df_split = df.withColumn("loc_split", split(df["location"], "a"))
display(df_split)

# 5. array_contains(): Check if array contains value
df_contains = df.withColumn("has_1", array_contains("skills", 1))
display(df_contains)

# 6. array_append(): Add element to end of array
df_append = df.withColumn("appended", array_append("skills", 9))
display(df_append)

# 7. array_compact(): Remove nulls from array
df_compact = df.withColumn("compact", array_compact("skills"))
display(df_compact)

# 8. array_distinct(): Remove duplicates from array
df_distinct = df.withColumn("distinct", array_distinct("skills"))
display(df_distinct)

# 9. array_except(): Diff of two arrays
df_except = df.withColumn("except_2", array_except("skills", array(lit(2))))
display(df_except)

# 10. array_intersect(): Common elements
df_intersect = df.withColumn("common", array_intersect("skills", array(lit(2), lit(4))))
display(df_intersect)

# 11. array_join(): Join array with delimiter
df_join = df.withColumn("joined", array_join("skills", "-"))
display(df_join)

# 12. array_max(): Maximum value in array
df_max = df.withColumn("max_val", array_max("skills"))
display(df_max)

# 13. array_min(): Minimum value in array
df_min = df.withColumn("min_val", array_min("skills"))
display(df_min)

# 14. array_position(): Get index of value (1-based)
df_position = df.withColumn("pos_2", array_position("skills", 2))
display(df_position)

# 15. array_remove(): Remove a specific value
df_remove = df.withColumn("removed_2", array_remove("skills", 2))
display(df_remove)

# COMMAND ----------

# Array() :— array function is used to create a new array from existing data from multiple columns

from pyspark.sql.functions import split, array, col

data = [(1, 'sarath', 'dotnet', 'azure'), (2, 'sai', 'gcp', 'AWS')]
schema = ['id', 'name', 'primary_skills', 'secondary_skills']
df = spark.createDataFrame(data, schema)
display(df)

df2 = df.withColumn('skills', array(col('primary_skills'), col('secondary_skills')))
display(df2)

#————————————————————————————————

from pyspark.sql.functions import concat, col, lit

# Example for concat with array elements
data = [(1, ['python', 'sql']), (2, ['java', 'scala'])]
schema = ['id', 'courses']
df1 = spark.createDataFrame(data, schema)
df2 = df1.withColumn('new_col', concat(col('courses').getItem(0), lit(','), col('courses').getItem(1)))
display(df2)

#—————————————————————————————————
            
# array_contains() :-  it checks the array has a given value or not, if value is there it will return True else False

from pyspark.sql.functions import split, array, col, array_contains

data = [(1, 'sarath', ['dotnet', 'azure']), (2, 'sai', ['gcp', 'AWS'])]
schema = ['id', 'name', 'primary_skills']
df = spark.createDataFrame(data, schema)
display(df)

df2 = df.withColumn('col_array_contains', array_contains(col('primary_skills'), 'AWS'))
display(df2)

#—————————————————————————————————————————————————————————————Maptype columns  ———————————————————————————————————

# Maptype:- map type is nothing but a dictionary in python. Map type is used to represent map key value pair

from pyspark.sql.types import MapType, StructType, StructField, StringType, IntegerType

data = [
    (1, 'sarath', {'height': '5.10', 'weight': '78', 'colour': 'white', 'eyes': 'brown'}),
    (2, 'lavanya', {'height': '5.6', 'weight': '65', 'colour': 'white', 'eyes': 'black'}),
    (3, 'charan', {'height': '5.9', 'weight': '65', 'colour': 'black', 'eyes': 'black'})
]
schema = ['id', 'name', 'details']
df = spark.createDataFrame(data, schema)
display(df)
df.printSchema()

schema1 = StructType([
    StructField('id', IntegerType()),
    StructField('name', StringType()),
    StructField('Details', MapType(StringType(), StringType()))
])
df1 = spark.createDataFrame(data, schema1)
display(df1)

df2 = df1.withColumn('face_clour', df1.Details['colour'])  #------accessing the maptype column for another column
display(df2)

#—————————————————————————————————————————————————————————————Maptype columns  ———————————————————————————————————

from pyspark.sql.functions import explode

df = spark.createDataFrame(data, schema)
display(df)

df2 = df.select('id', 'name', 'details', explode(col('details')))
display(df2)

#——————————————————————————————————————
# Mapkeys()— :- map keys show keys in a separate column

from pyspark.sql.functions import map_keys

df = spark.createDataFrame(data, schema)
display(df)

df2 = df.withColumn('map_keys_col', map_keys(col('details')))  #---- this will show only map keys in a column
display(df2)

#——————————————————————————————————————
# Mapvalues()— :- map values show values in a separate column

from pyspark.sql.functions import map_values

df = spark.createDataFrame(data, schema)
display(df)

df2 = df.withColumn('map_values_col', map_values(col('details')))  #---- this will show only map values in a column
display(df2)

# Extract individual map fields into separate columns
df_exploded = df.select(
    "id",
    "name",
    col("details").getItem("height").alias("height"),
    col("details").getItem("weight").alias("weight"),
    col("details").getItem("colour").alias("colour"),
    col("details").getItem("eyes").alias("eyes")
)
display(df_exploded)

# COMMAND ----------

#=============================================================================Row clause in pyspark ======================================================================================

from pyspark.sql import Row

row = Row('sarath', 'kunapareddy', 29)
print(row[0] + '------' + row[1] + '------' + str(row[2]))

s_row = Row(Fname='sarath', lname='kunapareddy')
print(s_row.Fname + '------' + s_row.lname + '------')

#——————————————————————————————————————
row1 = Row('sarath', 29)
row2 = Row('lavanya', 31)
row3 = Row('charan', 29)

data = [row1, row2, row3]

df = spark.createDataFrame(data, schema=['name', 'age'])
display(df)

#——————————————————————————————————————

# Note:— we can use ROW as a class  here is the below example

person = Row('name', 'age')

person1 = person('sarath', 30)
person2 = person('sai', 29)

print(person1.name, '---', person2.age)

# COMMAND ----------

from pyspark.sql.functions import lit, col

a = lit('abcd')

display(a)

data = [(1, 'sai', 'oracle'), (2, 'lavanya', 'powerbi'), (3, 'charan', 'informatica')]
schema = ['id', 'name', 'course']

df = spark.createDataFrame(data, schema)
display(df)

df2 = df.withColumn('new_id', lit('new_id'))

display(df2)

display(df2.select(col('name')))
# or
display(df2.select(df2['name']))
# or
display(df2.select(df2.name))

# COMMAND ----------

data = [(1, 'sai', 'M', 2000), (2, 'sarath', 'M', 3000), (3, 'kunapareddy', 'M', 4000)]

schema = ['id', 'name', 'gender', 'salary']

df = spark.createDataFrame(data, schema)

display(df)

#—————————————————————————————————————————————————

from pyspark.sql.functions import when, col

df1 = df.select(
    col('id'),
    col('name'),
    when(col('gender') == 'M', 'male')
        .when(col('gender') == 'F', 'female')
        .otherwise('unknown')
        .alias('gender'),
    col('salary')
)

display(df1)

# COMMAND ----------

data = [(1, 'sai', 'M', 2000), (2, 'sarath', 'M', 3000), (3, 'kunapareddy', 'M', 4000)]

schema = ['id', 'name', 'gender', 'salary']

df = spark.createDataFrame(data, schema)

display(df)

display(df.select(df.id, df.name, df.gender, df.salary))

#----alias function for columns 
display(df.select(df.id.alias('emp_id'), df.name.alias('emp_name'), df.gender.alias('emp_gender'), df.salary.alias('emp_salary')))

display(df.sort(df.id.desc())) #------------descending order

display(df.sort(df.id.asc())) #------------ascending order

df2 = df.select(df.id, df.name, df.gender, df.salary.cast('int'))  #----using a cast function to convert datatype

df.printSchema() #---before using cast 
df2.printSchema() #----after using cast

display(df.select(df.id, df.name, df.name, df.gender, df.salary)) #---like operator 

display(df.filter(df.name.like('k%'))) #------like operator to filter the data

# COMMAND ----------

data = [(1, 'sai', 'M', 2000), (2, 'sarath', 'M', 3000), (3, 'kunapareddy', 'M', 4000)]

schema = ['id', 'name', 'gender', 'salary']

df = spark.createDataFrame(data, schema)

display(df)

display(df.select(df.id, df.name, df.gender, df.salary))

display(df.filter(df.gender == 'M'))  #--------------filter the data to display males 

display(df.filter(df.name == 'sai'))  #--------------filter the data to display name with sai only

display(df.where(df.name == 'sai'))  #--------- we can use where also it is same functionallity

display(df.where((df.name == 'sai') & (df.salary == 2000)))  #--------- we can use where also it is same functionallity

# COMMAND ----------

data = [(1, 'sai', 'M', 2000), (2, 'sarath', 'M', 3000), (3, 'kunapareddy', 'M', 4000)]

schema = ['id', 'name', 'gender', 'salary']

df = spark.createDataFrame(data, schema)

display(df)

display(df.sort('name'))        #-------sort and order do the same 
display(df.orderBy('name'))     #-------sort and order do the same 

display(df.sort('name', 'salary'))        #-------sort and order do the same 
display(df.orderBy('name', 'salary'))     #-------sort and order do the same 

display(df.sort(df.name.desc(), df.salary.asc()))        #-------sort and order do the same 
display(df.orderBy(df.name.desc(), df.salary.asc()))     #-------sort and order do the same

# COMMAND ----------

# Note:- here. Union and union all. Performs both same, it won’t remove the duplicate rows. Like in SQL (union all)

data = [(1, 'sai', 'M', 2000), (2, 'sarath', 'M', 3000), (3, 'kunapareddy', 'M', 4000)]
schema = ['id', 'name', 'gender', 'salary']

df = spark.createDataFrame(data, schema)
df2 = df

display(df)
display(df2)

display(df.union(df2))  #-------------------- union operator

# unionAll is deprecated, use union instead
display(df.union(df2))  #-------------------- union operator

#=============================================================================group by in pyspark========================================================

data = [(1, 'sai', 'M', 2000), (2, 'sarath', 'M', 3000), (3, 'kunapareddy', 'M', 4000)]
schema = ['id', 'name', 'gender', 'salary']

df = spark.createDataFrame(data, schema)
df2 = df

display(df)
display(df2)

from pyspark.sql.functions import count, sum

display(df.groupBy('gender').count())

display(df.groupby(df.gender).agg(count('*').alias('count_of_emps'), sum(df.salary).alias('sum_of_salary')))

#=============================================================================unionByName in pyspark========================================================

data = [(1, 'sai', 'M', 2000), (2, 'sarath', 'M', 3000), (3, 'kunapareddy', 'M', 4000)]
data1 = [(1, 'sai', 'M', 2000), (2, 'sarath', 'M', 3000), (3, 'kunapareddy', 'M', 4000)]

schema = ['id', 'name', 'gender', 'salary']
schema1 = ['id', 'name', 'Identy', 'salary']

df = spark.createDataFrame(data, schema)
df2 = spark.createDataFrame(data1, schema1)

display(df)
display(df2)

# union will fail due to schema mismatch, use unionByName
display(df.unionByName(df2, allowMissingColumns=True))

#=============================================================================Select in pyspark========================================================

data = [(1, 'sai', 'M', 2000), (2, 'sarath', 'M', 3000), (3, 'kunapareddy', 'M', 4000)]
data1 = [(1, 'sai', 'M', 2000), (2, 'sarath', 'M', 3000), (3, 'kunapareddy', 'M', 4000)]

schema = ['id', 'name', 'gender', 'salary']
schema1 = ['id', 'name', 'Identy', 'salary']

df = spark.createDataFrame(data, schema)
df2 = spark.createDataFrame(data1, schema1)

display(df)
display(df2)

display(df.select('id', 'name', 'gender'))
# or
display(df2.select(df2.id, df2.name, df2.Identy))
# or
display(df2.select(['id', 'name', 'Identy', 'salary']))
# or
display(df2.select([cols for cols in df2.columns]))
# or
display(df2.select('*'))

#==========================================================================================================================================================================

from pyspark.sql.functions import approx_count_distinct, avg, collect_list, collect_set, count, countDistinct

simpleData = [("siva", "Dev", 1500),
              ("naga", "QA", 3000),
              ("raju", "Prod", 1500)]
schema = ["employee_name", "department", "salary"]

# Create DataFrame
df = spark.createDataFrame(data=simpleData, schema=schema)

# Perform aggregations
result_df = df.select(
    approx_count_distinct('salary').alias("approx_count_distinct"),
    avg('salary').alias("average_salary"),
    collect_list('salary').alias("salary_list"),
    collect_set('salary').alias("salary_set"),
    countDistinct('salary').alias("distinct_salary_count"),
    count('salary').alias("salary_count")
)

# Show result
display(result_df)

# COMMAND ----------

#--- df.cache()          #----------to cache the data 
#--df.unpersist()     #——————to remove the cache the data

#----from pyspark import StorageLevel
#-----df_persist = df.persist(StorageLevel.MEMORY_ONLY)

# COMMAND ----------

# In PySpark, an Accumulator is a shared, write-only variable used to perform distributed aggregation in a parallel computing environment. It is primarily used for counting or summing values across worker nodes without the need for synchronization.
# 🔹 Key Features
# * Efficient Aggregation: Accumulators aggregate values across tasks without requiring explicit synchronization.
# * Worker to Driver Communication: Workers update the accumulator, and the result is available on the driver.
# * Only Updatable in Actions: Transformations (e.g., map, filter) do not trigger accumulators—only actions (collect, count, etc.) do.

# Create an accumulator
accum = spark_session.sparkContext.accumulator(0)

# Sample RDD
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

# Function to add values to accumulator
def add_to_accum(x):
    global accum
    accum += x

# Apply function in an action
rdd.foreach(add_to_accum)

# Retrieve accumulator value
display(accum.value)

# COMMAND ----------

display(df.filter(df.gender == 'M'))  #--------------filter the data to display males 

display(df.filter(df.name == 'sai'))  #--------------filter the data to display name with sai only

display(df.where(df.name == 'sai'))  #--------- we can use where also it is same functionallity

display(df.where((df.name == 'sai') & (df.salary == 2000)))  #--------- we can use where also it is same functionallity
#——————————————————————————————————————————————————————————————————————————————————————————

display(df.sort('name', 'salary'))        #-------sort and order do the same 
display(df.orderBy('name', 'salary'))     #-------sort and order do the same 

display(df.sort(df.name.desc(), df.salary.asc()))        #-------sort and order do the same 
display(df.orderBy(df.name.desc(), df.salary.asc()))     #-------sort and order do the same 

#——————————————————————————————————————————————————————————————————————————————————————————

display(df.union(df2)) #-------------------- union operator

display(df.union(df2))  #-------------------- union operator

display(df1.union(df1).distinct()) #——————to show unique records

#——————————————————————————————————————————————————————————————————————————————————————————
from pyspark.sql.functions import col 

display(df.groupBy(df.gender).count())

from pyspark.sql.functions import col, count, sum

display(
    df.groupBy("gender")
      .agg(
          count('*').alias('count_of_emps'),
          sum("salary").alias('sum_of_salary')
      )
)
#——————————————————————————————————————————————————————————————————————————————————————————
display(df.union(df2))

display(df.unionByName(df2, allowMissingColumns=True))

#——————————————————————————————————————————————————————————————————————————————————————————

display(df.select('id', 'name', 'gender'))
                #or
display(df2.select(df2.id, df2.name, df2.Identy))
                #or
display(df2.select(['id', 'name', 'Identy', 'salary']))
                #or
display(df2.select([cols for cols in df2.columns]))
                #or
display(df2.select('*'))   

#——————————————————————————————————————————————————————————————————————————————————————————
