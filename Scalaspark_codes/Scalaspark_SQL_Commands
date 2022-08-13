

===== // Creates a DataFrame from CSV after reading in the file ======
Step #1 - Read The CSV File

val csvFile = "C:/sparkdata/employee.txt"
var tempDF = spark.read.option("sep", ",").csv(csvFile)
tempDF.printSchema()
tempDF.collect();
tempDF.take(10);

Step #2 - Use the File's Header

spark.read.option("sep", ",").option("header", "true").csv(csvFile).printSchema()

Step #3 - Infer the Schema
val csvFile = "C:/sparkdata/employee.txt"

spark.read.option("header", "true").option("sep", "\t").option("inferSchema", "true").csv(csvFile).printSchema()

Declare the schema.
// Required for StructField, StringType, IntegerType, etc.
import org.apache.spark.sql.types._
val csvSchema = StructType(
  List(
    StructField("empno", StringType, false),
    StructField("ename", StringType, false),
    StructField("job", StringType, false) 
	)
)
StructField("mgr", StringType, false),
	StructField("hiredate", StringType, false),
	StructField("sal", StringType, false),
	StructField("commission", StringType, false),
	StructField("deptno", StringType, false)
or 
case class csvSchema(empno:String, ename:String, job:String, mgr:String, hiredate:String, sal:String, comm:String, deptno:String)




Step #2
Read in our data (and print the schema).
spark.read.option("header", "true").option("sep", "\t").schema(csvSchema).csv(csvFile).printSchema()
  
  
  "empno","ename","job","mgr","hiredate","sal","comm","deptno"
  
  
  ========= Reading Perquate file
 
val parquetFile = "C:/sparkdata/userdata1.parquet"
val parquetDF= spark.read.parquet(parquetFile)
parquetDF.printSchema()   

val parquetSchema = StructType(
  List(
    StructField("project", StringType, false),
    StructField("article", StringType, false),
    StructField("requests", IntegerType, false),
    StructField("bytes_served", LongType, false),
	StructField("bytes_served12", LongType, false)
  )
)



  