//Author: Harshali Singh, Vishal Mehta

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.util.ArrayList


object Confusion{

	def main(args: Array[String]){

	/*if (args.length < 2) {

		System.err.println("Usage: MissedConnection <input> <output>")
		System.exit(1)
		}*/

		/*val testfile = args(0)
		val validatefile = args(1)*/

		val sparkConf = new SparkConf().
				    setAppName("Confusion").
		                    setMaster("local").
				    set("spark.executor.memory", "6g").
				    set("spark.driver.memory", "6g")

		val sc = new SparkContext(sparkConf)
		//val t0 = System.nanoTime()

		//Read test file
		val test = sc.textFile("testOutput").
					map(_.split("\t")).
					filter(_.size > 1).
					filter(_(0) != "header").
					map(testrows => (testrows(0), Array(testrows(1))))


		val valid = sc.textFile("a6validate/98validate.csv.gz").
						map(_.split(",")).
						filter(_.size > 1).
						filter(_(0) != "header").
						map(validrows => (validrows(0), Array(validrows(1))))

	/*	/*//Format and Trim lines
		val formatted = csv.map(line => line.replaceAll("\"", "").replaceAll(", ", ":"))*/
		val testrows = test.map(line => line.split("\t").map(_.trim))
		val testheader = testrows.first()

		//Remove Header
		val testdata = testrows.filter(line => line(0) != testheader(0))

		// Validate CSV File
		val validrows = validcsv.map(line => line.split("\t").map(_.trim))
		val validheader = validrows.first()

		//Remove Header
		val validdata = validrows.filter(line => line(0) != validheader(0))

		//Arriving Flights: (Carrier Year AiportCode, Array(Flight Date, Scheduled Arrival, Actual Arrival, Cancelled))
		val testRDD = testdata.map(

			testrows => (testrows(0), Array(testrows(1)))

			)

		//Departing Flights: (Carrier Year AiportCode, Array(Flight Date, Scheduled Departure, Actual Departure))
		val validRDD = validdata.map(

			validrows => (validrows(0), Array(validrows(1)))

			)*/

		//Performed Co-Group on F and G RDD
		val joinedRDD = test.join(valid)
		

		val matrixRDD = joinedRDD.map(key => {

			val (k, v) = key;
			val (predicted, actual) = v;

			var psize = predicted.size
			var asize = actual.size
			var tp = 0
			var fp = 0
			var tn = 0
			var fn = 0
			//println("#######"+predicted(0)  + "pred" +" ####"+ "act" + actual(0) + "----------------")
			//println(psize + "@#################" + asize)

					if (predicted(0) == actual(0)) {
						if(predicted(0) == "TRUE")
							tp = tp + 1
						else if (predicted(0) == "FALSE")
							tn = tn + 1
					}
					else if (predicted(0) != actual(0)) {
						if(predicted(0) == "TRUE")
							fp = fp + 1
						else if (predicted(0) == "FALSE")
							fn = fn + 1
					}
			//Emit a tuple containing Key and number of connections and missed connections
			(k, (tp.toString, tn.toString, fp.toString, fn.toString))
	
		}).collect()

		//Create Output RDD[String]
	    val output = matrixRDD.map (value => {

	   		val (k,v) = value;
	   		val (p1,p2,p3,p4) = v;

	   		//println(value)
			p1 + "\t" + p2 + "\t" + p3 + "\t" + p4	   		

	   	})

	    //Save Result to Disk
	   val result = sc.parallelize(output)
           result.saveAsTextFile("out")
        
    	   // Shut down Spark, avoid errors.
    	   sc.stop()

    }
}
 

