-----------------------------------
MAP:

Python:
x= sc.parallelize(["b", "a", "c"])
y= x.map(lambda z: (z, 1))
print(x.collect())
print(y.collect())

Scala:
val x= sc.parallelize(Array("b", "a", "c"))
val y= x.map(z => (z,1))
println(x.collect().mkString(", "))
println(y.collect().mkString(", "))

------------------------------------------------
FILTER

Python:
x= sc.parallelize([1,2,3])
y= x.filter(lambda x: x%2 == 1) #keep odd values
print(x.collect())
print(y.collect())

Scala:
val x= sc.parallelize(Array(1,2,3))
val y= x.filter(n => n%2 == 1)
println(x.collect().mkString(", "))
y.collect()
println(y.collect().mkString(", "))
------------------------------------------------
FLATMAP

Python:
x= sc.parallelize([1,2,3])
y= x.flatMap(lambda x: (x, x*100, 42))
print(x.collect())
print(y.collect())

Scala:
val x= sc.parallelize(Array(1,2,3))
val y= x.map(n => Array(n, n*100, 42))
x.collect()
y.collect()

//println(x)
//println(y)
//println(x.collect().mkString(", "))
//println(y.collect().mkString(", "))
------------------------------------------------
GROUPBY

Python:
x= sc.parallelize(['John', 'Fred', 'Anna', 'James'])
y= x.groupBy(lambda w: w[0])
print [(k, list(v)) for (k, v) in y.collect()]

Scala:
val x= sc.parallelize(Array("John", "Fred", "anna", "James"))
val y= x.groupBy(w => w.charAt(0))
x.collect()
y.collect()

println(y.collect().mkString(", "))

------------------------------------------------
GROUPBYKEY

Python:
x= sc.parallelize([('B',5),('B',4),('A',3),('A',2),('F',1)])
y= x.groupByKey()
print(x.collect())
print(list((j[0], list(j[1])) for j in y.collect()))

Scala:
val x= sc.parallelize(Array(('B',5),('B',4),('A',3),('A',2),('A',1)))
val y= x.groupByKey()
println(x.collect().mkString(", "))
println(y.collect().mkString(", "))
------------------------------------------------
REDUCEBYKEY VS GROUPBYKEY 


val words = Array("one", "two", "two", "three", "three", "three")

val wordPairsRDD= sc.parallelize(words).map(word => (word, 1))

val wordCountsWithReduce= wordPairsRDD
.reduceByKey(_ + _)
.collect()
val wordCountsWithGroup= wordPairsRDD
.groupByKey()
.map(t => (t._1, t._2.sum))
.collect()

------------------------------------------------
MAPPARTITIONS

Python:
x= sc.parallelize([1,2,3], 2)
deff(iterator):yield sum(iterator); yield 42
y= x.mapPartitions(f)
# glom() flattens elements on the same partition
print(x.glom().collect())
print(y.glom().collect())

Scala:
val x= sc.parallelize(Array(1,2,3), 2)
deff(i:Iterator[Int])={ (i.sum,42).productIterator}
val y= x.mapPartitions(f)
// glom() flattens elements on the same partition
val xOut= x.glom().collect()
val yOut= y.glom().collect()
------------------------------------------------
MAPPARTITIONSWITHINDEX

Python:
x= sc.parallelize([1,2,3], 2)
deff(partitionIndex, iterator):yield (partitionIndex, sum(iterator))
y= x.mapPartitionsWithIndex(f)
# glom() flattens elements on the same partition
print(x.glom().collect())
print(y.glom().collect())

Scala:
val x= sc.parallelize(Array(1,2,3), 2)
deff(partitionIndex:Int, i:Iterator[Int]) = {
(partitionIndex, i.sum).productIterator
}
val y= x.mapPartitionsWithIndex(f)
// glom() flattens elements on the same partition
val xOut= x.glom().collect()
val yOut= y.glom().collect()
------------------------------------------------
SAMPLE

Python:
x= sc.parallelize([1, 2, 3, 4, 5])
y= x.sample(False, 0.4, 42)
print(x.collect())
print(y.collect())

Scala:
val x= sc.parallelize(Array(1, 2, 3, 4, 5,6,7,8,9))
val y= x.sample(false, 0.2)
// omitting seed will yield different output
println(y.collect().mkString(", "))

------------------------------------------------
UNION

Python:
x= sc.parallelize([1,2,3], 2)
y= sc.parallelize([3,4], 1)
z= x.union(y)
print(z)

Scala:
val x= sc.parallelize(Array(1,2,3), 2)
val y= sc.parallelize(Array(3,4), 1)
val z= x.union(y)
//val z Out= z.glom().collect()
println(z.glom().collect())

------------------------------------------------
JOIN

Python:
x= sc.parallelize([("a", 1), ("b", 2)])
y= sc.parallelize([("a", 3), ("a", 4), ("b", 5)])
z= x.join(y)
print(z.collect())

Scala:
val x= sc.parallelize(Array(("a", 1), ("b", 2)))
val y= sc.parallelize(Array(("a", 3), ("a", 4), ("b", 5)))
val z= x.join(y)
println(z.collect().mkString(", "))
 
---------------------------------------
DISTINCT:
Python:
x= sc.parallelize([1,2,3,3,4])
y= x.distinct()
print(y.collect())

Scala:
val x= sc.parallelize(Array(1,2,3,3,4))
val y= x.distinct()
println(y.collect().mkString(", "))
---------------------------------------
COALESCE:
Python:
x = sc.parallelize([1, 2, 3, 4, 5], 3)
y= x.coalesce(2)
print(x.glom().collect())
print(y.glom().collect())

Scala:
val x= sc.parallelize(Array(1, 2, 3, 4, 5), 3)
val y= x.coalesce(2)
val xOut= x.glom().collect()
val yOut= y.glom().collect()
-----------------------------------
KEYBY:

Python:
x= sc.parallelize(['John', 'Fred', 'Anna', 'James'])
y= x.keyBy(lambda w: w[0])
print y.collect()

Scala:
val x= sc.parallelize(Array("John", "Fred", "Anna", "James"))
val y= x.keyBy(w => w.charAt(0))
println(y.collect().mkString(", "))
-----------------------------------
PARTITIONBY
Python:
x = sc.parallelize([('J','James'),('F','Fred'),
('A','Anna'),('J','John')], 3)
y= x.partitionBy(2, lambda w: 0 if w[0] < 'H' else 1)
print x.glom().collect()
print y.glom().collect()

Scala:
import org.apache.spark.Partitioner
val x= sc.parallelize(Array(('J',"James"),('F',"Fred"),
('A',"Anna"),('J',"John")), 3)
val y= x.partitionBy(new Partitioner() {
val numPartitions= 2
def getPartition(k:Any) = {
if (k.asInstanceOf[Char] < 'H') 0 else 1
}
})
val yOut= y.glom().collect()
-----------------------------------
ZIP:

Python:
x= sc.parallelize([1, 2, 3])
y= x.map(lambda n:n*n)
z= x.zip(y)
print(z.collect())

Scala:
val x= sc.parallelize(Array(1,2,3))
val y= x.map(n=>n*n)  
val z= x.zip(y)
println(z.collect().mkString(", "))
-----------------------------------
GETNUMPARTITIONS:

Python:
x= sc.parallelize([1,2,3], 2)
y= x.getNumPartitions()
print(x.glom().collect())
print(y)

Scala:
val x= sc.parallelize(Array(1,2,3,56,78,90),5)
val y= x.partitions.size
val xOut= x.glom().collect()
println(y)
-----------------------------------
COLLECT:

Python:
x= sc.parallelize([1,2,3], 2)
y= x.collect()
print(x.glom().collect())
print(y)

Scala:
valx= sc.parallelize(Array(1,2,3), 2)
valy= x.collect()
valxOut= x.glom().collect()
println(y)

------------------------------------
REDUCE:

Python:
x = sc.parallelize([1,2,3,4])
y= x.reduce(lambda a,b: a+b)
print(x.collect())
print(y)

Scala:
valx= sc.parallelize(Array(1,2,3,4))
valy= x.reduce((a,b) => a+b)
println(x.collect.mkString(", "))
println(y)
-------------------------------------------
AGGREGATE:

Python:
seqOp= lambda data, item: (data[0] + [item], data[1] + item)
combOp= lambda d1, d2: (d1[0] + d2[0], d1[1] + d2[1])
x= sc.parallelize([1,2,3,4])
y= x.aggregate(([], 0), seqOp, combOp)
print(y)

Scala:
defseqOp= (data:(Array[Int], Int), item:Int) => (data._1 :+ item, data._2 + item)
defcombOp= (d1:(Array[Int], Int), d2:(Array[Int], Int)) => (d1._1.union(d2._1), d1._2 + d2._2)
valx= sc.parallelize(Array(1,2,3,4))
valy= x.aggregate((Array[Int](), 0))(seqOp, combOp)
println(y)

-----------------------------------------
MAX:

Python:
x= sc.parallelize([2,4,1])
y= x.max()
print(x.collect())
print(y)

Scala:
valx= sc.parallelize(Array(2,4,1))
valy= x.max
println(x.collect().mkString(", "))
println(y)
---------------------------------------
SUM:

Python:
x= sc.parallelize([2,4,1])
y= x.sum()
print(x.collect())
print(y)

Scala:
valx= sc.parallelize(Array(2,4,1))
valy= x.sum
println(x.collect().mkString(", "))
println(y)
---------------------------------------
MEAN:

Python:
x= sc.parallelize([2,4,1])
y= x.mean()
print(x.collect())
print(y)

Scala:
valx= sc.parallelize(Array(2,4,1))
valy= x.mean
println(x.collect().mkString(", "))
println(y)
-----------------------------------------
STDEV:

Python:
x= sc.parallelize([2,4,1])
y= x.stdev()
print(x.collect())
print(y)

Scala:
valx= sc.parallelize(Array(2,4,1))
valy= x.stdev
println(x.collect().mkString(", "))
println(y)
-------------------------------------------
COUNTBYKEY:

Python:
x = sc.parallelize([('J', 'James'), ('F','Fred'),
('A','Anna'), ('J','John')])
y= x.countByKey()
print(y)

Scala:
val x= sc.parallelize(Array(('J',"James"),('F',"Fred"),
('A',"Anna"),('J',"John")))
val y= x.countByKey()
println(y)
---------------------------------------------
SAVEASTEXTFILE:

Python:
dbutils.fs.rm("/temp/demo", True)
x= sc.parallelize([2,4,1])
x.saveAsTextFile("/temp/demo")
y= sc.textFile("/temp/demo")
print(y.collect())

Scala:
dbutils.fs.rm("/home/hadoop/demo", true)
val x= sc.parallelize(Array(2,4,1))
x.saveAsTextFile("s3://my-script/repo")
val y= sc.textFile("file:/home/hadoop/")
println(y.collect().mkString(", "))

----------------------------------------



