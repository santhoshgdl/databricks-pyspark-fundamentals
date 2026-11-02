# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

# DBTITLE 1,Initialization from CSV File
df=spark.read.format('csv').option('inferSchema', True).option('header', True).load('/Volumes/workspace/default/raw_files/BigMart Sales.csv')
df.show()

# COMMAND ----------

# DBTITLE 1,Show DataFrame Output in Notebook Cell
df.display()

# COMMAND ----------

# DBTITLE 1,Display Total Number of Rows in DataFrame
# Get the total Count

print("Total rows:", df.count())

# COMMAND ----------

# DBTITLE 1,Describe Extended Schema for Big Mart Sales Table
df = spark.sql("""
    DESCRIBE EXTENDED workspace.default.big_mart_sales
""")

display(df)

# COMMAND ----------

# DBTITLE 1,List Raw Files with Their Sizes in Workspace Directory
files = dbutils.fs.ls("/Volumes/workspace/default/raw_files/")

for f in files:
    print(f.path, f.size)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading JSON

# COMMAND ----------

# DBTITLE 1,Load JSON Data into Spark DataFrame with Inferred Schem ...
df_json = spark.read.format('json').option('inferSchema', True)\
                    .option('header', True)\
                    .option('multiLine', False)\
                    .load('/Volumes/workspace/default/raw_files/drivers.json')

# COMMAND ----------

# DBTITLE 1,Display JSON DataFrame Content in Notebook Cell
df_json.display()

# COMMAND ----------

# DBTITLE 1,List Files in Raw Files Directory of Workspace
dbutils.fs.ls("/Volumes/workspace/default/raw_files/")

# COMMAND ----------

# DBTITLE 1,Display DataFrame Schema Structure and Types
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Define Schema for Big Mart Sales Data Attributes
my_ddl_schema = '''
    Item_Identifier string,
    Item_Weight string,
    Item_Fat_Content string,
    Item_Visibility double,
    Item_Type string,
    Item_MRP double,
    Outlet_Identifier string,
    Outlet_Establishment_Year integer,
    Outlet_Size string,
    Outlet_Location_Type string,
    Outlet_Type string,
    Item_Outlet_Sales double
'''

# COMMAND ----------

# DBTITLE 1,Load BigMart Sales Data into Spark DataFrame
df = spark.read.format('csv')\
    .schema(my_ddl_schema)\
    .option('header', True)\
    .load('/Volumes/workspace/default/raw_files/BigMart Sales.csv')


# COMMAND ----------

# DBTITLE 1,Print DataFrame Structure and Column Types
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Display DataFrame Content in Current Notebook Cell
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### StructType() Schema

# COMMAND ----------

# DBTITLE 1,Importing  types & Functions
from pyspark.sql.types import * 
from pyspark.sql.functions import * 


# COMMAND ----------

# DBTITLE 1,Load Big Mart Sales Data with Custom Schema in Spark


my_struct_schema = StructType([
                        StructField('Item_Identifier', StringType(), True),
                        StructField('Item_Weight', StringType(), True),
                        StructField('Item_Fat_Content', StringType(), True),
                        StructField('Item_Visibility', StringType(), True),
                        StructField('Item_Type', StringType(), True),
                        StructField('Item_MRP', StringType(), True),
                        StructField('Outlet_Identifier', StringType(), True),
                        StructField('Outlet_Establishment', StringType(), True),
                        StructField('Outlet_Size', StringType(), True),
                        StructField('Outlet_Location_Type', StringType(), True),
                        StructField('Outlet_Type', StringType(), True),
                        StructField('Item_Outlet_Sales', StringType(),True)        
                                    
])

df=spark.read.format('csv')\
    .schema(my_struct_schema)\
    .option('header', True)\
    .load("/Volumes/workspace/default/raw_files/BigMart Sales.csv")
df.printSchema()
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SELECT

# COMMAND ----------

# DBTITLE 1,Select Item Attributes from DataFrame for Analysis
df_sel = df.select('Item_Identifier', 'Item_Weight', 'Item_Fat_Content').display()

# COMMAND ----------

# DBTITLE 1,Select Item Attributes with Weight and Fat Content
df_select_col = df.select(col('Item_Identifier'), col('Item_Weight'), col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ALIAS

# COMMAND ----------

# DBTITLE 1,Select Item Identifier with Alias for DataFrame Display
# alias cannot be used without col object

df_select_alias = df.select(col('Item_Identifier').alias('Item_ID'))

display(df_select_alias)

# COMMAND ----------

# MAGIC %md
# MAGIC ### FILTER

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 1

# COMMAND ----------

# DBTITLE 1,Filter and Display Regular Item Fat Content from DataFr ...
df.filter(col("Item_Fat_Content") == "Regular").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario - 2

# COMMAND ----------

# DBTITLE 1,Filter Soft Drinks by Weight Less Than Ten Units
df.filter( (col('Item_Type') == "Soft Drinks") & (col('Item_Weight') < 10) ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario - 3

# COMMAND ----------

# DBTITLE 1,Filter DataFrame for Null Outlet Size in Specific Tiers
df.filter( (col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1', 'Tier 2'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumnRenamed

# COMMAND ----------

# DBTITLE 1,Rename Item Weight Column to Item wt in DataFrame
df.withColumnRenamed('Item_Weight', 'Item_wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 1

# COMMAND ----------

# DBTITLE 1,Add New Column Flag to DataFrame with Static Value
from pyspark.sql.functions import lit

df = df.withColumn('flag', lit("new"))

df.display()

# COMMAND ----------

# DBTITLE 1,Calculate Item Weight and MRP Product in DataFrame
df.withColumn('multiply', col('Item_Weight') * col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 2

# COMMAND ----------

# DBTITLE 1,Update Item Fat Content Labels to Shortened Versions
df.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), "Regular", "Reg"))\
    .withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), "Low Fat", "LF")).display()

# COMMAND ----------

# DBTITLE 1,Update Item Weight Column Data Type in DataFrame
df = df.withColumn('Item_Weight', col('Item_Weight').cast(IntegerType()))

df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### SORTING

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Scenario - 1

# COMMAND ----------

# DBTITLE 1,Sort DataFrame by Item Weight in Descending Order
df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Scenario - 2

# COMMAND ----------

# DBTITLE 1,Sort DataFrame by Item Visibility in Ascending Order
df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Scenario - 3

# COMMAND ----------

# DBTITLE 1,Sort DataFrame by Visibility and Weight Attributes
df.sort(['Item_Visibility', 'Item_Weight'],ascending=[0,0]).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Scenario - 4

# COMMAND ----------

# DBTITLE 1,Sort DataFrame by Item Weight and Visibility Attributes
df.sort(['Item_Weight', 'Item_Visibility'], ascending=[0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### LIMIT

# COMMAND ----------

# DBTITLE 1,Preview First 10 Rows of DataFrame in Notebook Cell
df.limit(10).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### DROP

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 1

# COMMAND ----------

# DBTITLE 1,Remove Item Visibility Column from DataFrame Output
df.drop('Item_Visibility').display()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Scenario - 2

# COMMAND ----------

# DBTITLE 1,Remove Unnecessary Columns from DataFrame for Analysis
df.drop('Item_Visibility', 'Item_Type').display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### DROP_DUPLICATES

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 1

# COMMAND ----------

# DBTITLE 1,Remove Duplicate Records from DataFrame and Display
df.dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 2
# MAGIC

# COMMAND ----------

# DBTITLE 1,Remove Duplicate Item Types from DataFrame
df.dropDuplicates(subset = ['Item_Type']).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### UNION & UNION_BYNAME

# COMMAND ----------

# DBTITLE 1,Combine Two DataFrames and Display Unified Output
data1 = [('1', 'kad'), ('2', 'sid')]
schema1 = "id STRING, name STRING"

data2 = [('3', 'Rahul'), ('4', 'Jas')]
schema2 = 'id STRING, name STRING'

df1 = spark.createDataFrame(data1, schema1)
df2 = spark.createDataFrame(data2, schema2)

# df1.display()
# df2.display()

df1.union(df2).display()



# COMMAND ----------

# MAGIC %md
# MAGIC ### UNION BY NAME
# MAGIC

# COMMAND ----------

# DBTITLE 1,Combine and Display Two Spark DataFrames with Union
data1 = [('kad', '1'), ('2', 'sid')]
schema1 = "name STRING, id STRING"

data2 = [('3', 'Rahul'), ('4', 'Jas')]
schema2 = 'id STRING, name STRING'

df1 = spark.createDataFrame(data1, schema1)
df2 = spark.createDataFrame(data2, schema2)

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### STRING FUNCTIONS 
# MAGIC ##### INITCAP()
# MAGIC ##### UPPER()
# MAGIC ##### LOWER()

# COMMAND ----------

# DBTITLE 1,Show Variations of Item_Type Formatting in DataFrame
df.select(initcap('Item_Type')).limit(2).display()
df.select(lower('Item_Type')).limit(2).display()
df.select(upper('Item_Type')).limit(2).display()
df.select(upper('Item_Type').alias('Upper_Item_Type')).limit(2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATE FUNCTIONS 
# MAGIC
# MAGIC #### CURRENT DATE

# COMMAND ----------

# DBTITLE 1,Add Current Date Column to DataFrame for Analysis
df = df.withColumn('current_date', current_date())
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### date_add() Function

# COMMAND ----------

# DBTITLE 1,Add Week After Date Column to DataFrame
df = df.withColumn('week_after', date_add('current_date', 7))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Date_Sub() Function

# COMMAND ----------

# DBTITLE 1,Calculate Date for One Week Prior and Display DataFrame
df = df.withColumn('week_before', date_sub('current_date', 7))
df.display()

# COMMAND ----------

# DBTITLE 1,Add Week Before Date Column to DataFrame
df = df.withColumn('week_before', date_add('current_date', -7))
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### DateDIFF()
# MAGIC

# COMMAND ----------

# DBTITLE 1,Calculate Date Difference from Current Date to Week Aft ...
df = df.withColumn('date_diff', datediff('week_after', 'current_date'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATE_FORMAT()

# COMMAND ----------

# DBTITLE 1,Format Date Column for Week Before in DataFrame
# dd	Day of month	20
# MM	Month of year	10
# yyyy	Calendar year	2026
# YYYY	Week-based year	Depends on the week and calendar rules
# HH	Hour (24-hour clock)	14
# mm	Minute	35
# ss	Second	42
df = df.withColumn('week_before', date_format('week_before', 'dd-MM-yyyy'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling NULLs

# COMMAND ----------

# MAGIC %md
# MAGIC #### DRopping NULLs
# MAGIC

# COMMAND ----------

# DBTITLE 1,Remove Rows with All Null Values from DataFrame
df.dropna('all')display()

# COMMAND ----------

# DBTITLE 1,Display DataFrame After Dropping Missing Values
df.dropna('any').display()

# COMMAND ----------

# DBTITLE 1,Filter DataFrame to Remove Rows with Missing Outlet Siz ...
#GOOD TO USE 
df.dropna(subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Filling NULLs

# COMMAND ----------

# DBTITLE 1,Format Item Weight as String and Fill Missing Values
df = df.withColumn('Item_Weight', col('Item_Weight').cast('string'))

df=df.fillna("Not Availble").display()


# COMMAND ----------

# DBTITLE 1,Fill Missing Outlet Size Values with Not Available Tag
df.fillna("Not Available", subset=['Outlet_Size']).display()

# COMMAND ----------

# DBTITLE 1,Display DataFrame Contents in Notebook Cell Output
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SPLIT & INDEXING

# COMMAND ----------

# MAGIC %md
# MAGIC #### SPLIT

# COMMAND ----------

# DBTITLE 1,Split Outlet Type into Individual Components in DataFra ...
df.withColumn('Outlet_Type', split('Outlet_Type', ' ')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### INDEXING

# COMMAND ----------

# DBTITLE 1,Extract Outlet Type from DataFrame Column
df.withColumn('Outlet_Type', split('Outlet_Type', ' ')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXPLODE

# COMMAND ----------

# DBTITLE 1,Expand Outlet Type Column by Splitting on Space
df_exp = df.withColumn('Outlet_Type', split('Outlet_Type', ' '))
df_exp.display()

# COMMAND ----------

# DBTITLE 1,Expand Outlet Type Column with Exploded Values
df_exp.withColumn('Outlet_Type', explode('Outlet_Type')).display()

# COMMAND ----------

# DBTITLE 1,Add Type1 Flag Column to Outlet DataFrame
df_exp.withColumn('Type1_Flag', array_contains('Outlet_Type', 'Type1')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### GROUP BY 
# MAGIC #### Scenario - 1

# COMMAND ----------

# DBTITLE 1,Aggregate Item MRP by Type in Spark DataFrame
df.groupBy('Item_Type').agg(sum('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 2

# COMMAND ----------

# DBTITLE 1,Calculate Average Item MRP by Item Type in DataFrame
df.groupBy('Item_Type').agg(avg('Item_MRP').alias('Avg_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 4

# COMMAND ----------

# DBTITLE 1,Aggregate Item MRP by Type and Outlet Size
df.groupBy('Item_Type', 'Outlet_Size').agg(sum('Item_MRP').alias('Sum_of_MRP')).display()

# COMMAND ----------

# DBTITLE 1,Aggregate Item MRP by Type and Outlet Size
df.groupBy('Item_Type', 'Outlet_Size').agg(sum('Item_MRP'), avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### COLLECT_LIST

# COMMAND ----------

# DBTITLE 1,Create Spark DataFrame for User Book Interactions
data = [
    ('user1', 'book1'),
    ('user2', 'book2'),
    ('user2', 'book2'),
    ('user2', 'book4'),
    ('user3', 'book1')
]

schema = 'user string, book string'

df_book = spark.createDataFrame(data, schema)

df_book.display()

# COMMAND ----------

# DBTITLE 1,Group Users with Their Favorite Books in List Format
df_book.groupBy('user').agg(collect_list('book')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### PIVOT

# COMMAND ----------

# DBTITLE 1,Average Item MRP by Outlet Size and Item Type
df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###    WHEN - OTHERWISE

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 1

# COMMAND ----------

# DBTITLE 1,Set Vegetarian Flag Based on Item Type in DataFrame
df = df.withColumn('veg_flag', when(col('Item_Type') == 'Meat', 'Non-Veg').otherwise('Eww Vegetarian'))

# COMMAND ----------

# DBTITLE 1,Categorize Vegetarian Items by Price Range
df.withColumn('veg_exp_flag', when( ( (col('veg_flag') == "Eww Vegetarian") & (col('Item_MRP') < 100) ), 'Veg_Inexpensive' )\
                             .when( ( (col('veg_flag') == 'Eww Vegetarian') & (col('Item_MRP') > 100) ), 'Veg_Expensive')\
                             .otherwise('Non-Vegetarian')
               ).display()

# COMMAND ----------

# DBTITLE 1,Joins Sample Dataframe
dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### INNER JOINS 

# COMMAND ----------

# DBTITLE 1,Inner Join
# if we dont mention the column name at the start, the spark will face the error of ambiguity
df1.join(df2, df1['dept_id'] == df2['dept_id'], 'inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### LEFT JOIN

# COMMAND ----------

# DBTITLE 1,Left Join
df1.join(df2, df1['dept_id'] == df2['dept_id'], 'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### RIGHT JOIN

# COMMAND ----------

# DBTITLE 1,Right Join
df1.join(df2, df1['dept_id'] == df2['dept_id'], 'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ANTI-JOIN

# COMMAND ----------

# DBTITLE 1,Anti Join
df1.join(df2, df1['dept_id'] == df2['dept_id'], 'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WINDOW FUNCTIONS

# COMMAND ----------

# DBTITLE 1,Importing Window Function
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC #### ROW_NUMBER()

# COMMAND ----------

# DBTITLE 1,Row Number
df.withColumn('rowCol', row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### RANK VS DENSE_RANK

# COMMAND ----------

# DBTITLE 1,Rank Vs Dense Rank
df.withColumn('rank', rank().over(Window.orderBy(col('Item_Identifier').desc())))\
    .withColumn('dense_rank', dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### CUMULATIVE SUM

# COMMAND ----------

# DBTITLE 1,Calculate Cumulative Sum of Item MRP by Item Type
df.withColumn('cumulative_sum', sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()

# COMMAND ----------

# DBTITLE 1,Calculate Cumulative Sum by Item Type in DataFrame
df.withColumn('cum_sum', sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding, Window.currentRow))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### USER DEFINED FUNCTIONS (UDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### STEP - 1

# COMMAND ----------

# DBTITLE 1,Define Simple Squaring Function for Input Value
def my_func(x):
    return x*x

# COMMAND ----------

# MAGIC %md
# MAGIC #### STEP - 2

# COMMAND ----------

# DBTITLE 1,Create User Defined Function for Data Processing
my_udf = udf(my_func)

# COMMAND ----------

# MAGIC %md
# MAGIC #### STEP - 3

# COMMAND ----------

# DBTITLE 1,Add New Column Using UDF on Item MRP and Display Output
df.withColumn('mynewCol', my_udf('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA WRITING

# COMMAND ----------

# MAGIC %md
# MAGIC #### CSV

# COMMAND ----------

# DBTITLE 1,Display Available SQL Catalogs
# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# DBTITLE 1,Fetch Current Database Catalog Name
# MAGIC %sql
# MAGIC SELECT current_catalog();

# COMMAND ----------

# DBTITLE 1,Database Schema Overview
# MAGIC %sql
# MAGIC SHOW SCHEMAS;

# COMMAND ----------

# DBTITLE 1,Show Volume Information in Default Workspace
# MAGIC %sql
# MAGIC SHOW VOLUMES IN workspace.default;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Save DataFrame as CSV File with Header Option
df.write \
    .format('csv') \
    .option('header', 'true') \
    .mode('overwrite') \
    .save('/Volumes/workspace/default/raw_files/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC #### APPEND()

# COMMAND ----------

# DBTITLE 1,Append DataFrame to CSV File in Workspace Directory
df.write.format('csv')\
    .mode('append')\
    .save('/Volumes/workspace/default/raw_files/data.csv')

# COMMAND ----------

# DBTITLE 1,Append DataFrame to CSV File in Specified Path
df.write.format('csv')\
    .mode('append')\
    .option('path','/Volumes/workspace/default/raw_files/data.csv')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### OVERWRITE()

# COMMAND ----------

# DBTITLE 1,Save DataFrame to CSV File in Overwrite Mode
df.write.format('csv')\
    .mode('overwrite')\
    .option('path', '/Volumes/workspace/default/raw_files/data.csv')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### ERROR()

# COMMAND ----------

# DBTITLE 1,Save DataFrame as CSV to Specified File Path
df.write.format('csv')\
    .mode('error')\
    .option('path', '/Volumes/workspace/default/raw_files/data.csv')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### IGNORE()

# COMMAND ----------

# DBTITLE 1,Save DataFrame as CSV File in Specified Path
df.write.format('csv')\
    .mode('ignore')\
    .option('path', '/Volumes/workspace/default/raw_files/data.csv')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### PARQUET

# COMMAND ----------

# DBTITLE 1,Save DataFrame to Parquet Format in Specified Location
df.write.format('parquet')\
    .mode('overwrite')\
    .option('path', '/Volumes/workspace/default/raw_files/data.csv')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### TABLE

# COMMAND ----------

# DBTITLE 1,Save DataFrame as Parquet Table in Overwrite Mode
df.write.format('parquet')\
    .mode('overwrite')\
    .saveAsTable('my_table')

# COMMAND ----------

# MAGIC %md 
# MAGIC ### SPARK SQL

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Creating Temp View

# COMMAND ----------

# DBTITLE 1,Create Temporary View from DataFrame for Analysis
df.createTempView('my_view') 

# COMMAND ----------

# DBTITLE 1,Retrieve All Records from My View in Database
# MAGIC %sql
# MAGIC SELECT * FROM my_view;

# COMMAND ----------

# DBTITLE 1,View Metadata for All Available Database Views
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM INFORMATION_SCHEMA.VIEWS;

# COMMAND ----------

# DBTITLE 1,List Views in dbo Schema from Information Schema
# MAGIC %sql
# MAGIC SELECT
# MAGIC     TABLE_SCHEMA,
# MAGIC     TABLE_NAME
# MAGIC FROM INFORMATION_SCHEMA.VIEWS
# MAGIC WHERE TABLE_SCHEMA = 'dbo';

# COMMAND ----------

# DBTITLE 1,Fetch and Display Data from My View in Spark DataFrame
df_sql = spark.sql('SELECT * FROM my_view;')
df_sql.display()