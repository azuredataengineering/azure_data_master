# Databricks notebook source
data1=([(1,'maheer',2000,2),(2,'waffa',3000,1),(3,'abcd',1000,4)])

schema1=['id','name','salary','dep']

data2=([(1,'it'),(2,'hr'),(3,'payroll')])

schema2=['id','name']

empdf=spark.createDataFrame(data1,schema1)

deptdf=spark.createDataFrame(data2,schema2)

empdf.show()
deptdf.show()

empdf.join(deptdf,empdf.dep==deptdf.id,'inner').show()
empdf.join(deptdf,empdf.dep==deptdf.id,'left').show()
empdf.join(deptdf,empdf.dep==deptdf.id,'right').show()
empdf.join(deptdf,empdf.dep==deptdf.id,'full').show()


#----leftsemi join ----it will do same as equi join but display left table data only 
empdf.join(deptdf,empdf.dep==deptdf.id,'leftsemi').show()
#----leftanti join-----it will do same as equi join but display not matching rows  from. the left table 
empdf.join(deptdf,empdf.dep==deptdf.id,'leftanti').show()

#-----self join  ----
#000empdf.join(deptdf,empdf.dep==deptdf.id,'').show() ---need. to check 


# COMMAND ----------

#---pivot----the data 
data1=([(1,'maheer',2000,2),(2,'waffa',3000,1),(3,'abcd',1000,4),(4,'abcd',1000,4),(5,'abcd',1000,4),(6,'waffa',3000,1)])

schema1=['id','name','salary','dep']

empdf=spark.createDataFrame(data1,schema1)

display(empdf)
empdf.groupBy('dep').pivot('name').sum('salary').show()



# COMMAND ----------

#===================================================================================unpivot in pyspark==============================================================================================
# Note:—  here unpivot is not available  in sql and same way here also we don’t have unpivot, but we  use stack() 

data=([('IT',8,5),('payroll',3,2),('HR',2,4)])

schema=['dep','male','female']

df=spark.createDataFrame(data,schema)

display(df)

from pyspark.sql.functions import expr

unpivot=df.select('dep',expr("stack(2,'male',male,'female',female) as (gender,count)"))

display(unpivot)


# COMMAND ----------

#==================================================================================fill() & fillna() functions in PySpark============================================================================

# Note: —  fill and. Fillna  function  is  like a nvl function  like. To replace. The null. Value. 

data=([(1,'sarath',3000,'parchur'),(2,'lavanya',5000,'triputahi'),(2,'sarath',8000,''),(4,None,3000,'')])

schema=['id','name','salary','location']

df=spark.createDataFrame(data,schema)

df.fillna('hhhshs').show()

df.fillna('hhhshs',['id','name','location']).show()
df2=df.na.fill('unknown',['id','name','location'])


# — here fill. And.  Fillna  functions. Works like. Both. Are same 

# COMMAND ----------

#==================================================================================sample() function in PySpark ============================================================================

# Sample function:-  to get the sample dataset from  large dataset  

df=spark.range(1,101)
df.show()

df1=df.sample(fraction=0.1,seed=123)
df2=df.sample(fraction=0.1,seed=123)

display(df1)

display(df2)


# COMMAND ----------

# — here fill. And.  Fillna  functions. Works like. Both. Are same 



# Note:—  when we use seed  the value of both will be same and the length will be also  same 

#==================================================================================collect() function in PySpark ============================================================================


# Collect:-  it will retrieves all the elements into a single array dataframe  like an array ( here the data is collected to a single note , so keep in mind it will not recommend for  large data set )


data=([(1,'sarath',3000,'parchur'),(2,'lavanya',5000,'triputahi'),(2,'sarath',8000,''),(4,'',3000,'')])

schema=['id','name','salary','location']

df=spark.createDataFrame(data,schema)

display(df)


df1=df.collect()

print(df1)

print(df1[0]) #————it will print the first element based on index

print(df1[0][1]) #————it will print the first element based on index —>it will give the name column in first row 




# COMMAND ----------

#==================================================================================transform  function in PySpark ============================================================================

data=([(1,'sarath',3000,'parchur'),(2,'lavanya',5000,'triputahi'),(2,'sarath',8000,''),(4,'',3000,'')])

schema=['id','name','salary','location']

df=spark.createDataFrame(data,schema)

display(df)



from pyspark.sql.functions import upper,lower
from pyspark.sql.types import *

def covert_string_upper(df):
  return df.withColumn('name',upper(df.name))

def covert_string_lower(df):
  return df.withColumn('name',lower(df.name))
def double_salary_amount(df):
  return df.withColumn('new_slary',df.salary*df.salary)


df1=df.transform(covert_string_upper)
df2=df.transform(covert_string_lower)
df3=df.transform(double_salary_amount)

df1.show()

df2.show()

df3.show()


#==================================================================================pyspark.sql.functions.transform()   function in PySpark ============================================================================

data=([(1,'sarath',3000,['parchur','chicago']),(2,'lavanya',5000,['triputahi','chicago']),(2,'sarath',8000,['','saint']),(4,'',3000,['','ssss'])])

schema=['id','name','salary','location']

df=spark.createDataFrame(data,schema)

display(df)

from pyspark.sql.functions import transform,upper


df.select('id','name','salary',transform('location',lambda x: upper(x)).alias('namddde')).show()

def convert_upper(x):
  return upper(x)

df.select(transform('location',convert_upper)).show()


# COMMAND ----------


#==================================================================================create. Or. replace  temp_view()   function in PySpark ============================================================================

# Note:- it is  used to create a tableview   by using sql query  

# Advantages:— advantages  of spark ,  you can  work with sql along with  datafromes. That means if  you are comfortable  with sql , you can create temporary view on dataframe by using create or replace tempview()
#			and use sql to select and manipulate  data.
#                          
#                         Temp Views are session scoped and cannot be shared betweeen the sessions.


data=([(1,'sarath',3000,'parchur'),(2,'lavanya',5000,'triputahi'),(2,'sarath',8000,''),(4,'',3000,'')])

schema=['id','name','salary','location']

df=spark.createDataFrame(data,schema)

display(df)
df.select('id','name','salary','location').show()

df.createOrReplaceTempView  ('emp_vvvvv')


df1=spark.sql("select *  from emp_vvvvv")
df1.show()



#==================================================================================create. Or. replace  temp_Global_view()   function in PySpark ========================================================================



# Note:— it is used to create temp views or tables  globally, when can be accessed across the sessions with in the spark application
	
#		To query these tables, we need  append global_temp.tablename 

data=([(1,'sarath',3000,'parchur'),(2,'lavanya',5000,'triputahi'),(2,'sarath',8000,''),(4,'',3000,'')])

schema=['id','name','salary','location']

df=spark.createDataFrame(data,schema)

df.show()

df.createOrReplaceGlobalTempView('sarath_emp')


df1=spark.sql("select *  from global_temp.sarath_emp")

df1.show()


# COMMAND ----------

#————————————————————————————————————————————————————————————————————————————————————————————————————————————

# %sql

# select * from global_temp.sarath_emp


# Note:—  we can access this. Table in  another notebooks since it is a global temporary table  



# To see the tables in the list —> spark.catalog.listTables(‘global_temp’) 

# spark.catalog.listTables(‘global_temp’) 
# spark.catalog.listColumns(global_temp’) 
# spark.catalog.listDatabases(‘global_temp’) 
# spark.catalog.listFunctions(‘global_temp’) 



# COMMAND ----------










#=================================================================================UDF. (User defined function   function in PySpark ========================================================================


# UDF(. User defined function  —> it is similar to  SQL functions  , we define some logic in functions and store them in database and use  them in queries                                                           similar to that we can write our custom logic. In python  function and. Register it with pyspark using  UDF() function 


data=([(1,'sarath',3000,'parchur'),(2,'lavanya',5000,'triputahi'),(2,'sarath',8000,''),(4,'',3000,'')])

schema=['id','name','salary','location']

df=spark.createDataFrame(data,schema)

df.show()

def totalpay(s,b):
  return s+b

from pyspark.sql.functions import  udf
from pyspark.sql.types import IntegerType

total_payment=udf(lambda s,b: totalpay(s,b),IntegerType())

df.withColumn('totpay',total_payment(df.salary,df.salary)).show()

#=================================================================================UDF. (User defined function   function in PySpark ========================================================================







#=================================================================================RDD object to data frame  in PySpark ========================================================================


data=[(1,2),(3,4)]


rdd=spark.sparkContext.parallelize(data)

print(rdd.collect())
print(type(rdd))

df=rdd.toDF(schema=['id','num']) #———————this is a function to create a rdd to dataframe

df.show()

df1=spark.createDataFrame(rdd,schema=['id','num']) #———in this way also we can create a rdd to dataframe

df1.show()

#=================================================================================map transformation in RDD    in PySpark ========================================================================

# Note —— only map is used in  RDD. Only  there is not map function in data frame


# Definition : — RDD transformation is used to apply fucntion(lambda ) on every element of RDD and returns new RDD., Dataframe doesn’t. have map() transformation to use with dataframe you need to generate RDD first 



data=[('maheer','shaik'),('wafa','shaik')]

rdd=spark.sparkContext.parallelize(data)

print(rdd.collect())

rdd1=rdd.map(lambda x: x + (x[0]+' '+x[1],))

print(rdd1.collect())

#=================================================================================flat map  in RDD    in PySpark ========================================================================


# Note: —  flatmap() transformation  in pyspark , it is a transformation that flattens the RDD (array/map dataframe columns) after applying  the function on every element. And returns  a new spark RDD

#              It is not available in data frames , exlode() fucntion can be used in dat frames to flatten arrays.


data=[('maheer sheik'),('wafer shaik')]

rdd=spark.sparkContext.parallelize(data)

print(rdd.collect()) 

for item in rdd.collect():
    print(item)

rdd1=rdd.map(lambda x: x.split(' '))

for item in rdd1.collect():
    print(item)
rdd2=rdd.flatMap(lambda x: x.split(' '))

for item in rdd2.collect():
    print(item) 

#=================================================================================partition by function in   in RDD    in PySpark ========================================================================

data=[(1,'sarath','male','IT'),(2,'wafa','male','HR'),(3,'asi','female','IT')]

schema=['id','name','gender','dept']

df=spark.createDataFrame(data,schema)

df.show()

df.write.parquet('/Users/lakshmivenkatasaisarathkunapareddy/Desktop/Tables/sss/partion_table_by_dept.parquet',mode="overwrite",partitionBy='dept')

#————————————data to read  from. The folder 

dff=spark.read.parquet('/Users/lakshmivenkatasaisarathkunapareddy/Desktop/Tables/sss/partion_table_by_dept.parquet')

dff.show()

#——————————data to read from. The. Particular. Portion folder 

dff=spark.read.parquet('/Users/lakshmivenkatasaisarathkunapareddy/Desktop/Tables/sss/partion_table_by_dept.parquet/dept=IT')

dff.show()

#==============================================================================sub_partitoning in pyspark =============================================

data=[(1,'sarath','male','IT'),(2,'wafa','male','HR'),(3,'asi','female','IT')]

schema=['id','name','gender','dept']

df=spark.createDataFrame(data,schema)

df.show()

df.write.parquet('/Users/lakshmivenkatasaisarathkunapareddy/Desktop/Tables/sss/partion_table_by_dept_gender.parquet',mode="overwrite",partitionBy=['dept','gender'])

dff=spark.read.parquet('/Users/lakshmivenkatasaisarathkunapareddy/Desktop/Tables/sss/partion_table_by_dept_gender.parquet/dept=IT/gender=male')

dff.show()

# COMMAND ----------


