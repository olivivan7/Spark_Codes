// Multi-RDD transformation examples
1. Create 2 RDD such as rdd1 and rdd2 from array of string 

val rdd1=sc.parallelize(Array("Chicago", "Boston", "Paris", "San Francisco", "Tokyo"));

val rdd2=sc.parallelize(Array("San Francisco", "Boston", "Amsterdam", "Mumbai", "McMurdo Station"))

2. Check the contnets of the rdd1 and rdd2
rdd1.collect()

rdd2.collect()

3. Union of both rdd1 and rdd2

rdd1.union(rdd2).collect();


rdd1.subtract(rdd2).collect()

rdd1.zip(rdd2).collect();



// flatmap and distinct example

1. Set the file path in a varioable call filepath 
val filepath="file:/home/training/training_materials/data/purplecow.txt"

2. Create mydata_distinct RDD which consists of distinct values only 

val mydata_distinct=sc.textFile(filepath).flatMap(line =>line.split(' ')).distinct()

3. Check the contents of distinct value from mydata_distinct RDD 
mydata_distinct.collect


val a=Array("Chicago", "Boston", "Paris", "San Francisco", "Tokyo");

val rdd1=sc.parallelize(a);



