------------------------------------
Importing required scala libraries 
------------------------------------
Scala:

import org.apache.spark.sql.SQLContext
val sqlCtx = new SQLContext(sc)
import sqlCtx._
------------------------------------
Reading json file 
------------------------------------
val file="/user/training/people.json"
//val file="C:/sparkdata/people.json"

val MyDF=load((file),"json")

val peopleDF =sqlCtx.jsonFile(file);
---val peopleDF =sqlCtx.jsonFile("/user/people.json");
--C:\sparkdata\people.json
OR 

-- val peopleDF1 = load("/user/data/people.json","json")

val peopleDF1 = load("/user/training/people.json","json")




------------------------------------
Loading from a MySQL database 
------------------------------------

--val accountsDF= sqlCtx.load("jdbc",Map("url"-> "jdbc:mysql://dbhost/dbname?user=..&password=...","dbtable"-> "accounts"))
--val custDF = sqlCtx.load("jdbc",Map("url"-> "jdbc:MariaDB//mydb/dbname?user=root&password=root","dbtable"-> "cust2019"))
MariaDB 10.3
// Example JDBC data source
--val accountsDF=sqlCtx.load("jdbc",   
--    Map("url"-> "jdbc:mysql://localhost/loudacre?user=training&password=training",
 --          "dbtable" -> "accounts"))

------------------------------------
Generic Load Function
------------------------------------
val myDF = sqlCtx.load("myfile.avro","com.databricks.spark.avro")

------------------------------------
Basic Operations of Data Frame 
------------------------------------
val peopleDF = sqlCtx.jsonFile("/user/root/people.json")
peopleDF.dtypes.foreach(println)

peopleDF.count()

peopleDF.show(3)

Data Frame queries 
peopleDF.limit(3).show

peopleDF.select("age").show()
peopleDF.select("name","age").show();
peopleDF.where($"age > 21")
------------------------------------
Querying Data Frame using columns
------------------------------------
val ageDF = peopleDF.select($"age")

OR 

val ageDF =peopleDF.select(peopleDF("age"))

------------------------------------
Querying Data frame using columns 
------------------------------------
peopleDF.select(peopleDF("name"),peopleDF("age")+10)

peopleDF.sort(peopleDF("age").desc)

------------------------------------
SQL Queries 
------------------------------------
peopleDF.registerTempTable("people")
sqlCtx.sql("""select * from people where name LIKE "A%" """)

------------------------------------
Data Frames to RDD
------------------------------------
val peopleRDD= peopleDF.rdd

val peopleRDD=peopleDF.rdd
val peopleByPCode = peopleRDD.map(row => (row(2),row(1))).groupByKey().collect();



