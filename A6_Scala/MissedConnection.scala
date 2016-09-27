//Author: Harshali Singh, Vishal Mehta

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.util.ArrayList

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.Days

/**
 * Missed Connection -- The scala program that computes 
 * the number of missed connections and the percentage of missed connections
 * per airline, per year.
 */

object MissedConnection{

	def main(args: Array[String]){

	if (args.length < 2) {

		System.err.println("Usage: MissedConnection <input> <output>")
		System.exit(1)
		}

		val inputPath = args(0)
		val outPath = args(1)

		val sparkConf = new SparkConf().
		setAppName("Missed Connection").
		setMaster("local")
		val sc = new SparkContext(sparkConf)
		val t0 = System.nanoTime()

		//Read csv files
		val csv = sc.textFile(inputPath)

		//Format and Trim lines
		val formatted = csv.map(line => line.replaceAll("\"", "").replaceAll(", ", ":"))
		val rows = formatted.map(line => line.split(",").map(_.trim))
		val header = rows.first()

		//Remove Header
		val data = rows.filter(line => line(0) != header(0))

		//Filter data based on Sanity Test
		val saneData = data.filter(line => sanityTest(line))

		//Arriving Flights: (Carrier Year AiportCode, Array(Flight Date, Scheduled Arrival, Actual Arrival, Cancelled))
		val fRDD = saneData.map(

			row => (row(6)+"\t"+row(0)+"\t"+row(23), Array(row(5), row(40), row(41), row(47)))

			)

		//Departing Flights: (Carrier Year AiportCode, Array(Flight Date, Scheduled Departure, Actual Departure))
		val gRDD = saneData.map(

			row => (row(6)+"\t"+row(0)+"\t"+row(14), Array(row(5), row(29), row(30)))

			)

		//Performed Co-Group on F and G RDD
		val newRDD = (fRDD cogroup gRDD)

		val missedRDD = newRDD.map(key => {

			val (k, v) = key;
			val (f, g) = v;

			var cons = 0
			var missedcons = 0

			val format = DateTimeFormat.forPattern("yyyy-MM-dd");

			//Array lists to store F and G data 
			val foutvalues = new ArrayList[String];
			val ginvalues = new ArrayList[String];

			//Map on F Data 
			val fdata = f.map (fentry => {
				
				val fDate = fentry(0);

				//Arrival Times
				var schArr = appendString(fentry(1));
				var actArr = appendString(fentry(2));

				//Cancelled
				var fCancelled = fentry(3);

				foutvalues.add(fDate + "\t" + schArr + "\t" + actArr + "\t" + fCancelled)

				})
			
			//Map on G Data 
			val gdata = g.map (gentry => {
				
				val gDate = gentry(0);

				//Departure Times
				var schDep = appendString(gentry(1));
				var actDep = appendString(gentry(2));

				ginvalues.add(gDate + "\t" + schDep + "\t" + actDep)

				})
			
			val fsize = foutvalues.size
			val gsize = ginvalues.size

			//Iterating over F data Arraylist 
			for(i <- 0 until fsize) {				

				val fsplit = foutvalues.get(i).split('\t')

				val strFdate = fsplit(0)
				val fDate = format.parseDateTime(strFdate)

				val schArr = fsplit(1)
				val actArr = fsplit(2)
				val fCancelled = fsplit(3).toInt

					//Iterating over G data Arraylist 
					for (j <- 0 until gsize) {

						val gsplit = ginvalues.get(j).split('\t')

						val strGdate = gsplit(0)
						val gDate = format.parseDateTime(strGdate)

						val schDep = gsplit(1)
						val actDep = gsplit(2)

						//Flights on same day
						if(getDiffDays(fDate, gDate) == 0) {

							if(isSameDayCon(schDep, schArr)) {

								cons += 1
								
								// if flight on same day is missed
								if(isSameDayMissedCon(actDep, actArr, fCancelled)) {

									missedcons += 1

								} 
							}
							
						}
						
						//Flights rolled over to Next day
						else if (getDiffDays(fDate, gDate) == 1 && gDate.isAfter(fDate)) {

							if(isNextDayCon(schDep, schArr)) {

									cons += 1

								// if flight on different day is missed
								if (isNextDayMissedCon(actDep, actArr, fCancelled)) {

									missedcons += 1
										
								} 
								
							} 

						}						

					}
			}			
			
			//Emit a tuple containing Key and number of connections and missed connections
			(k, (cons.toString,missedcons.toString))
	
		}).collect()


		//Create Output RDD[String]
	    val output = missedRDD.map (value => {

	   		val (k,v) = value;
	   		val (p1,p2) = v;

	   		k + "\t" + p1 + "\t" +	 p2

	   	})

	    //Save Result to Disk
	   	val result = sc.parallelize(output)
        result.saveAsTextFile(outPath)
        
    	// Shut down Spark, avoid errors.
    	sc.stop()

    	// Total running time of program
    	val t1 = System.nanoTime()
    	println("Total time:======>" + ((t1 - t0)/1000000) + " ms")

    }

   /**
	* Method to check if given two times on different days, the flight is a connection or not
	* @param    String        scheduled arrival and departure times of two flights
	* @return   Boolean       value depending on whether the flight is a connection or not
	*
	**/

    def isNextDayCon(schDep : String, schArr : String) : Boolean = {

    	var strSchDep = appendString(schDep);
    	var strSchArr = appendString(schArr);

		//Scheduled Departure
		var schDepHour = getHour(strSchDep);
		var schDepMin = getMin(strSchDep);

		//Scheduled Arrival
		var schArrHour = getHour(strSchArr);
		var schArrMin = getMin(strSchArr);								

		//Scheduled Layover
		var schLayover = _getLayover(schArrHour, schArrMin, schDepHour, schDepMin);

		if(schLayover <= 360 && schLayover >= 30){

			return true;
		}

		return false;

	}

   /**
	* Method to check if given two times on different days, the flight is a missed connection or not
	* @param    String        actual arrival and departure times of two flights
	* @return   Boolean       value depending on whether the flight is a missed connection or not
	*
	**/
	def isNextDayMissedCon(actDep : String, actArr : String, fCancelled : Int) : Boolean = {

		var strActDep = appendString(actDep);
		var strActArr = appendString(actArr);

		//Actual Departure
		var actDepHour = getHour(strActDep);
		var actDepMin = getMin(strActDep);

		//Actual Arrival
		var actArrHour = getHour(strActArr);
		var actArrMin = getMin(strActArr);	

		//Actual Layover
		var actLayover = _getLayover(actArrHour, actArrMin, actDepHour, actDepMin);

		// Is a missed connection if Flight F is cancelled 
		// or actual layover is less than 30 minutes
		if((fCancelled == 1) || actLayover < 30){

			return true;
		}

		return false;
	}

   /**
	* Method to check if given two times on same day, the flight is a connection or not
	* @param    String        scheduled arrival and departure times of two flights
	* @return   Boolean       value depending on whether the flight is a connection or not
	*
	**/
	def isSameDayCon(schDep : String, schArr : String) : Boolean = {

		var schDepTime = convertTime(schDep);
		var schArrTime = convertTime(schArr);
		var schLayover = getLayover(schDepTime, schArrTime);

		if(schLayover <= 360 && schLayover >= 30){

			return true;
		}

		return false;

	}

   /**
	* Method to check if given two times on same day, the flight is a missed connection or not
	* @param    String        actual arrival and departure times of two flights
	* @return   Boolean       value depending on whether the flight is a missed connection or not
	*
	**/

	def isSameDayMissedCon(actDep : String, actArr : String, fCancelled : Int) : Boolean = {

		var actDepTime = convertTime(actDep);
		var actArrTime = convertTime(actArr);

		//Actual Layover
		var actLayover = getLayover(actDepTime, actArrTime);
		
		// Is a missed connection if Flight F is cancelled 
		// or actual layover is less than 30 minutes
		if((fCancelled == 1) || actLayover < 30){

			return true;
		}

		return false;

	}


   /**
	* Method to get the number of days between two dates
	* @param    DateTime      scheduled departure flight dates of two flights
	* @return   Int           number of days between the two dates
	*
	**/
	def getDiffDays(start : DateTime, end : DateTime) : Int = {

		var diffDays = Days.daysBetween(start.toLocalDate(), end.toLocalDate()).getDays()   //In Days

		return diffDays

	}

   /**
	* Method to get the Hour of a given time in HHmm format
	* @param    String      Time in hhmm format
	* @return   Int         the hour part
	*
	**/
	def getHour(strTime : String) : Int = {

		if(strTime.length() == 4){
			return (strTime.substring(0, 2)).toInt
			} else return 0;

		}

   /**
	* Method to get the Minute of a given time in HHmm format
	* @param    String      Time in hhmm format
	* @return   Int         the minute part 
	*
	**/
	def getMin(strTime : String) : Int = {

		if(strTime.length() == 4){
			return (strTime.substring(2, 4)).toInt
			} else return 0;
	}

   /**
	* Method to get the layover time between two given flight times on same day
	* @param    Int      Time in minutes
	* @return   Int      Layover time 
	*
	**/
	def getLayover(depTime : Int, arrTime : Int) : Int = {

		return math.abs(depTime - arrTime);

	}

   /**
	* Method to get the layover time between two given flight times on different days
	* @param    Int      Time in minutes
	* @return   Int      Layover time 
	*
	**/
	def _getLayover(arrHour : Int, arrMin : Int, depHour : Int, depMin : Int) : Int = {

		var _arrHour = math.abs(24-arrHour);
		var _hour = _arrHour + depHour;
		var _min = math.abs(depMin - arrMin);
		var _layover = _hour * 60 - _min;

		return _layover;

	}


   /**
	* Method to enforce the sanity test for every row (flight data).
	* @param    Array[String] each row of flight data
	* @return   Boolean       value depending on whether a flight passes sanity test or not.
	*
	**/
	def sanityTest (row : Array[String]) : Boolean = {

		if (row.length != 110){
			return false;
		}

		try {

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

   /**
	* Method to convert time in hh:mm format to Minutes.
	* @param String s (time in hh:mm)
	* @return int    time in minutes
	*/
	def convertTime(s : String) : Int = {

		var result = appendString(s);
		var time = 0;

		if(result.length() == 4){
			var hours = (result.substring(0,2)).toInt;
			var minutes = (result.substring(2,4)).toInt;

			time = hours * 60 + minutes;

		}

		return time;

	}

   /**
	* Method to append number of zeros to the time based on its length
	* @param String s (time in hh:mm)
	* @return String appended string with zeros.
	*/
	def appendString(s : String) : String = {

		var result = "";
		var s_new = s.trim();
		if(s_new.length()< 4 && !s_new.isEmpty()){

			result = "0" + s_new;
		}
		else if (s_new.isEmpty()) {

			result = "0000"

		} else {
			
			result = s_new
		}

		return result;
	}
}

