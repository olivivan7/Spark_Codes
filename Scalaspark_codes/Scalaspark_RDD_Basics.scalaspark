// Example code from the Getting Started chapter

// Return the name of the spark shell's Spark Context appname
sc.appName()

Start Spark Shell 
Copy purplecow to hdfs 

hadoop fs -put purplecow.txt /user/training/purplecow.txt


// Create an RDD from a text file
var mydata = sc.textFile("/user/training/purplecow.txt")

// return the number of lines in the RDD
mydata.count()

// print the first to records in the RDD
for (line <- mydata.take(2)) { println(line) }

// Create a new RDD by mapping the existing one to upper case
var mydata_uc = mydata.map(line => line.toUpperCase)

// Create an RDD with only lines being with the letter 'I'
var mydata_filt = mydata_uc.filter(line => line.startsWith("I"))

// Return the number of lines in the final RDD
mydata_filt.count()

// Show RDD lineage
mydata_filt.toDebugString

// Define a custom function and use it in a map
def toUpper(s: String) : String = { s.toUpperCase }
mydata.map(toUpper).take(2)

// Two versions of the same thing: convert to upper case and display two lines
mydata.map(line => line.toUpperCase()).take(2)
mydata.map(_.toUpperCase()).take(2)
