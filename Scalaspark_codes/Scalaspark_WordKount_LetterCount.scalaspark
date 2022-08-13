// Example: Use an accumulator to count the total number of letters in a file while counting words

// Create an RDD with the words in the file

val filename="file:/root/sparklabs/dev1/examples/example-data/purplecow.txt"

val words = sc.textFile(filename).flatMap(line => line.split("\\W"))
  


//Count the letters in all the words

val lettercount = sc.accumulator(0)

words.foreach(w => lettercount.add(w.length))

println("Total letters: " + lettercount.value)