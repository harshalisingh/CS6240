// Author: Harshali Singh, Vishal Mehta	
package main

	import scala.util.control._
	import scala.collection.mutable.ListBuffer
	import scala.util.Random
	import java.io.IOException
	import org.apache.hadoop.conf.Configuration
	import org.apache.hadoop.conf.Configured
	import org.apache.hadoop.fs.Path
	import org.apache.hadoop.io._
	import org.apache.hadoop.mapreduce.Job
	import org.apache.hadoop.mapreduce._
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
	import scala.collection.JavaConversions._
	import java.io._

	/*
	 *  The map reduce program that emits the median
	 * ticket prices for each airline in the year 2015 using Fast Median Algorithm. 
	 * 
	 */
	 object FastMedianComparison {
	 	def main(args:Array[String]) {

			/*if (args.length != 2) {
				System.err.println("Usage: MedianComparison <input-path> <output-path>");
				return 1;
				}*/

				val job = Job.getInstance();
				val t0 = System.nanoTime();
	
				job.setJar("FastMedianComparison.jar");
				job.setJobName("FastMedianComparison");

				//println("------Calling mapper----" );
				job.setMapperClass(classOf[FastMedianMapper]);
				job.setReducerClass(classOf[FastMedianReducer]);
				job.setMapOutputKeyClass(classOf[Text]);
				job.setMapOutputValueClass(classOf[Text]);

				job.setOutputKeyClass(classOf[Text]);
				job.setOutputValueClass(classOf[Text]);

				FileInputFormat.addInputPath(job, new Path(args(0)));
				FileOutputFormat.setOutputPath(job, new Path(args(1)));
				job.waitForCompletion(true);
				val t1 = System.nanoTime();
				println((t1 - t0)/1000);
				val fw = new FileWriter("testfast.txt", true);
			try {
				fw.write(String.valueOf((t1 - t0)/1000));			
				}
				finally fw.close();
			}
		/*
		 * Mapper program to read the entire data and filter out the flights which
		 * are not active in the year 2015 and passes the key value pair to the
		 * reduce program. Key is the carrier code + Month and Value is the Avg. ticket
		 * price for that particular airline.
		 * 
		 */
		 class FastMedianMapper extends Mapper[Object, Text, Text, Text] {
		 	type Context = Mapper[Object, Text, Text, Text]#Context

		 	override def map(_K : Object, value : Text, context : Context) {
				//println("In Map");

				var carrierCode = new String();
				var avgTicketPrice = new String();
				var year = 0;
				var month = new String();

				var line = value.toString();
				var newLine = line.replaceAll("\"", "");
				var formattedLine = newLine.replaceAll(", ", ":");
				var row = new Array[String](120);			
				row = formattedLine.split(",");

				if (sanityTest(row)) {
					carrierCode = row(6);
					avgTicketPrice = row(109);
					month = row(2);

					year = Integer.parseInt(row(0));

					if(year == 2015) {
						var outKey = new Text();
						var outValue = new Text();

						outKey = createKey(carrierCode, month);
						outValue = createValue(avgTicketPrice);

						context.write(outKey, outValue);

					}
				}

			}

			def createKey(carrier : String, month : String) : Text = {
				
				var returnKey = new Text();
				if (!carrier.isEmpty() && !month.isEmpty()) {
					returnKey = new Text(carrier.trim() + "\t" + month.trim());
				}
				return returnKey;
			}		
			

			def createValue(avgPrice : String) : Text = {
				var returnValue = new Text();
				if (!avgPrice.isEmpty()) {
					returnValue = new Text(avgPrice);
				}
				return returnValue;
			}

			//Sanity Test
			def sanityTest (row : Array[String]) : Boolean = {

			//System.out.prvarln(row.length);
			
			if (row.length != 110){
				return false;
			}
			
			try {
				//String carrierCode = row[8];
				//System.out.prvarln(carrierCode);
				
				//Time is converted to minutes
				var CRSArrTime = if (row(40).isEmpty()) 0 else convertTime(row(40));
				var CRSDepTime = if (row(29).isEmpty()) 0 else convertTime(row(29));
				var CRSElapsedTime = (row(50)).toFloat.toInt;
				var timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime;
				
				//CRSArrTime and CRSDepTime should not be zero
				
				if (CRSArrTime == 0 || CRSDepTime == 0) {
					return false;
				}	
				
				//timeZone % 60 should be 0
				if (timeZone % 60 != 0) {
					return false;
				}
				
				var OriginAirportId = (row(11)).toFloat.toInt;
				var DestAirportId = (row(20)).toFloat.toInt;
				var OriginAirportSeqId = (row(12)).toFloat.toInt;
				var DestAirportSeqId = (row(21)).toFloat.toInt;
				var OriginCityMarketId = (row(13)).toFloat.toInt;
				var DestCityMarketId = (row(22)).toFloat.toInt;
				var OriginStateFips = (row(17)).toFloat.toInt;
				var DestStateFips = (row(26)).toFloat.toInt;
				var OriginWac = (row(19)).toFloat.toInt;
				var DestWac = (row(28)).toFloat.toInt;
				
				//AirportID,  AirportSeqID, CityMarketID, StateFips, Wac should be larger than 0
				if (OriginAirportId <= 0 || DestAirportId <= 0 || OriginAirportSeqId <= 0 || 
					DestAirportSeqId <= 0 || OriginCityMarketId <= 0 || DestCityMarketId <= 0 || 
					OriginStateFips <= 0 || DestStateFips <= 0 || OriginWac <= 0 || DestWac <= 0) {
					return false;
				}
				
				//Origin, Destination,  CityName, State, StateName should not be empty
				if (row(14).isEmpty() || row(23).isEmpty() || row(15).isEmpty() || row(24).isEmpty() ||
					row(16).isEmpty() || row(25).isEmpty() || row(18).isEmpty() || row(27).isEmpty()) {
					return false;
				}
				
				//Flights that are not Cancelled
				var cancelled = (row(47)).toFloat.toInt;
				if (cancelled != 1) {
					var arrTime = if(row(41).isEmpty()) 0 else convertTime(row(41));
					var depTime = if(row(30).isEmpty()) 0 else convertTime(row(30));
					var actualElapsedTime = (row(51)).toFloat.toInt;
					
					//ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
					var diff = arrTime - depTime - actualElapsedTime - timeZone;
					if (diff != 0 && diff % 1440 != 0) {
						return false;
					}
					
					var arrDelay = (row(42)).toFloat.toInt;
					var arrDelayMinutes = (row(43)).toFloat.toInt;
					
					//if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
					//if ArrDelay < 0 then ArrDelayMinutes should be zero
					if (arrDelay > 0 && arrDelay != arrDelayMinutes) {
						return false;
						} else if (arrDelay < 0 && arrDelayMinutes != 0) {
							return false;
						}

					//if ArrDelayMinutes >= 15 then ArrDel15 should be true
					if (arrDelayMinutes >= 15 && (row(44)).toFloat.toInt != 1) {
						return false;
					}
				}
				
			}
			catch { case ex: NumberFormatException => {
				return false;
			}	
		}
		return true;	
	}


	def convertTime(s : String) : Int = {

		var result = "";
		var time = 0;
		var s_new = s.trim();
		if(s_new.length()< 4){
			result = "0" + s_new;
		}
		else {
			result = s_new;
		}

		if(result.length() == 4){
			var hours = Integer.parseInt(result.substring(0,2));
			var minutes = Integer.parseInt(result.substring(2,4));

			time = hours * 60 + minutes;

		}
		return time;
	}		
}

        /*
		 * Reducer program to reduce the <Key, List<Value>> pairs based on key
		 * 
		 */

		 class FastMedianReducer extends Reducer[Text, Text, Text, Text] {
		 	type Context = Reducer[Text, Text, Text, Text]#Context

			//ArrayList<Float> priceList = new ArrayList<Float>();
			
			//var medianvalues = new ListBuffer[Float]();

			override def reduce(key : Text, values: java.lang.Iterable[Text], context: Context) {

				var prices = new ListBuffer[Float]();
				// System.out.println(values.toString());
				//println("In Reduce");
				var count = 0;
				for (value <- values) {
					// System.out.println("Inside " +value.toString());
					var price = value.toString();
					//println("price" +price);
					prices += price.toFloat;
					count += 1;
				}

				var priceSize  = prices.length;

				var medianPrice=0.0;
				if(priceSize % 2 == 0){

					medianPrice = (quickSelect(prices, (priceSize/2)) +  quickSelect(prices, (priceSize/2 - 1)))/ 2;

				}
				else{
					medianPrice = quickSelect(prices, (priceSize/2 - 1));
				}

				context.write(key, new Text(count + "\t" + String.valueOf(medianPrice)));
			}

		/*
		 * This program determines the kth order statistic (the kth largest number in a
         * list) in O(n) time in the average case and O(n^2) time in the worst case.  It
         * achieves this through the Quickselect algorithm.
		 * 
		 */

		 def quickSelect(medianvalues: ListBuffer[Float], k : Int) : Float = {


		 	var result = kthLargest(medianvalues, 0, medianvalues.length, k);
		 	return result;
		 }


			/**
     		* Recursively determines the kth order statistic for the given list.
     		*
     		* @param medianvalues          The list.
     		* @param low                   The left index of the current sublist.
     		* @param high                  The right index of the current sublist.
     		* @param k                     The k value to use.
     		* @return                      The kth order statistic for the list.
     		*/
     		
     		def kthLargest(medianvalues : ListBuffer[Float], low : Int, high: Int, k : Int) : Float = {

     			if (low < high) {
     				var pivotLoc = partition(medianvalues, low, high);
     				var temp = medianvalues(pivotLoc);
     				medianvalues(pivotLoc) = medianvalues(low);
     				medianvalues(low) = temp;
     				if (pivotLoc == k - 1) {

     					return medianvalues(pivotLoc);

     					} else if (pivotLoc > k - 1) {
     						return kthLargest(medianvalues, low, pivotLoc, k);
     						} else {
     							return kthLargest(medianvalues, pivotLoc + 1, high, k);
     						}
     					}

     					return -1;

     				}

					 /**
                      * Randomly partitions a set about a pivot such that the values to the left
                      * of the pivot are less than or equal to the pivot and the values to the
                      * right of the pivot are greater than the pivot.
                      *
                      * @param values          The list.
                      * @param low             The left index of the current sublist.
                      * @param high            The right index of the current sublist.
                      * @return                The index of the pivot.
                      */


                      def partition(values : ListBuffer[Float], low : Int, high : Int) : Int = {


                      	var pivot = values(low);
                      	var left = low;

                      	var i=0;

                      	for (i <- (low + 1) until high) {
                      		if (values(i) < pivot) {
                      			left +=  1;
                      			var temp = values(i);
                      			values(i) = values(left);
                      			values(left) = temp;
                      		}
                      	}

                      	return left;
                      }
                  }
              }
