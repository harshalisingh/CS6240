package missed;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import missed.FlightUtils;

/**
 * Missed Connection -- The map reduce program that computes 
 * the number of missed connections and the percentage of missed connections
 * per airline, per year.
 * @author Harshali Singh, Vishal Mehta
 */
public class MissedConnection extends Configured implements Tool {

	/**
	 * Mapper class to read the entire data and filter out the flights which
	 * do not pass sanity test and output appropriate Key, Value pairs.
	 * Key is the Carrier code, Year and the origin or destination. 
	 * Value is FlightDate, Scheduled Departure Time, Actual Departure Time
	 * and Cancelled value. 
	 */

	public static class ConnectionMapper extends Mapper<Object, Text, Text, Text> {

		/**
		 *	Map Method to process and emit the key value pair described above to the reducer.
		 *  @param   Object  key
		 *  @param   Text    value 
		 *  @return  void
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			//Split the record
			String line = value.toString();
			String newLine = line.replaceAll("\"", "");
			String formattedLine = newLine.replaceAll(", ", ":");
			String[] row = formattedLine.split(",");

			//Check if the flight passed sanity test.
			if (FlightUtils.sanityTest(row)) {

				String carrierCode = row[6];
				String year = row[0];
				String flightDate = row[5];

				String origin = row[14];
				String dest = row[23];

				//Scheduled Departure and Arrival Time
				String schDepTime = row[29];
				String schArrTime = row[40];

				//Actual Departure and Arrival Time
				String actDepTime = row[30];
				String actArrTime = row[41];

				String cancelled = row[47];

				Text fOutKey = null;
				Text fOutValue = null;
				Text gOutKey = null;
				Text gOutValue = null;

				fOutKey = createKey(carrierCode, year, dest);
				fOutValue = arrCreateValue(flightDate, schArrTime, actArrTime, cancelled);

				gOutKey = createKey(carrierCode, year, origin);
				gOutValue = depCreateValue(flightDate, schDepTime, actDepTime, cancelled);

				//F data
				context.write(fOutKey, fOutValue);

				//G data
				context.write(gOutKey, gOutValue);

			}

		}

		/** Method to Create Mapper output Key
		 *  @param   String    carrier code, year, airport code
		 *  @return  String    Mapper output Key
		 */
		private Text createKey(String carrier, String year, String airport) {
			Text returnKey = null;
			if (!carrier.isEmpty() && !year.isEmpty() && !airport.isEmpty()) {
				returnKey = new Text(carrier.trim() + "\t" + year.trim() + "\t" + airport.trim());
			}
			return returnKey;
		}

		/** Method to Create Mapper output Value
		 *  @param    flight date, scheduled arrival time, actual arrival time, flight is cancelled or not
		 *  @return   Mapper output value for arriving flight
		 */
		private Text arrCreateValue(String flightDate, String schArrTime, String actArrTime, String cancelled) {
			Text returnValue  = new Text("F" + "\t" + flightDate + "\t" + schArrTime + "\t" + actArrTime + "\t" + cancelled);

			return returnValue;
		}

		/** Method to Create Mapper output Value
		 *  @param    flight date, scheduled arrival time, actual arrival time, flight is cancelled or not
		 *  @return   Mapper output value for departing flight
		 */
		private Text depCreateValue(String flightDate, String schDepTime, String actDepTime, String cancelled) {
			Text returnValue  = new Text("G" + "\t" + flightDate + "\t" + schDepTime + "\t" + actDepTime + "\t" + cancelled);

			return returnValue;
		}


	}

	/**
	 * Reducer class to emit the number of connections and number of missed connections 
	 * per carrier, per year and per airport hop connection. The <Key, List<Values>> are for carriers
	 * which have  F.Destination = G.Origin 
	 * 
	 */
	public static class ConnectionReducer extends Reducer<Text, Text, Text, Text>{

		/**
		*	Reduce Method to process the input from mapper and emit the key value pairs.
		* 	Key is Carrier code, year and origin/destination.
		*	Value is total number of connections and number of missed connections.
		*  @param   Text 		   key
		*  @param   Iterable<Text> values 
		*  @return  void
		*/
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException, IndexOutOfBoundsException {

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

			int numCons = 0;
			int missedCons = 0;
			
			List<Text> outValues = new ArrayList<Text>();
			List<Text> inValues = new ArrayList<Text>();
			
			String f = new String("F");   //Incoming Flight
			String g = new String("G");   //Outgoing Flight

			for (Text value: values){

				String[] split = value.toString().split("\t");

				if(split[0].trim().equals(f)){

					outValues.add(new Text(value));

				}
				
				if(split[0].trim().equals(g)){

					inValues.add(new Text(value));
				}

			}
			


			for(Text outValue : outValues){

				try{

					String[] outsplit = outValue.toString().split("\t");

						Date fdate = sdf.parse(outsplit[1]);   //Flight Date
						String schArr = outsplit[2];
						String actArr = outsplit[3];
						int fCancelled = Integer.parseInt(outsplit[4]);

						for(Text inValue :  inValues){

							String[] insplit = inValue.toString().split("\t");

								Date gdate = sdf.parse(insplit[1]);         //Flight Date
								String schDep = insplit[2];
								String actDep = insplit[3];

								long diffDays = getDiffDays(fdate, gdate);       //Difference in Days

								//Flights on same day
								if(diffDays == 0){

									int SchDepTime = FlightUtils.convertTime(schDep);
									int ActDepTime = FlightUtils.convertTime(actDep);

									int SchArrTime = FlightUtils.convertTime(schArr);
									int ActArrTime = FlightUtils.convertTime(actArr);

									int schLayover = getLayover(SchDepTime, SchArrTime);
									int actLayover = getLayover(ActDepTime, ActArrTime);

									// Is a connection if scheduled departure of G is <= 6 hours 
									// and >= 30 minutes after the scheduled arrival of F.
									if(schLayover <= 360 && schLayover >= 30){

										numCons += 1;

										// Is a missed connection if Flight F is cancelled 
										// or actual layover is less than 30 minutes 
										if((fCancelled == 1) || actLayover < 30){

											missedCons += 1;

										}


									}								

								} 
								//Flights rolled over to next day
								else if(diffDays == 1 && gdate.after(fdate)){


									String strSchDep = FlightUtils.appendString(schDep);
									String strActDep = FlightUtils.appendString(actDep);

									String strSchArr = FlightUtils.appendString(schArr);
									String strActArr = FlightUtils.appendString(actArr);

									//Scheduled Departure
									int schDepHour = getHour(strSchDep);
									int schDepMin = getMin(strSchDep);

									//Scheduled Arrival
									int schArrHour = getHour(strSchArr);
									int schArrMin = getMin(strSchArr);		

									//Actual Departure
									int actDepHour = getHour(strActDep);
									int actDepMin = getMin(strActDep);

									//Actual Arrival
									int actArrHour = getHour(strActArr);
									int actArrMin = getMin(strActArr);							

									//Scheduled Layover
									int schLayover = _getLayover(schArrHour, schArrMin, schDepHour, schDepMin);

									//Actual Layover
									int actLayover = _getLayover(actArrHour, actArrMin, actDepHour, actDepMin);

									//Connection
									if(schLayover <= 360 && schLayover >= 30){

										numCons += 1;

										//Missed Connection
										if((fCancelled == 1) || (actLayover < 30)){

											missedCons += 1;
											
										
										}


									}



								}

							}
						}
				
				catch(ParseException ex){

				}



			}

			context.write(key, new Text(numCons + "\t" + missedCons));


		}
		/**
		*	Method to calculate difference in days between arriving and departing flights.
		*  @param   Date fdate
		*  @param   Date gdate 
		*  @return  long diffdays (difference in days)
		*/

		private static long getDiffDays(Date fdate, Date gdate){

			long diff = Math.abs(gdate.getTime() - fdate.getTime());  //Date difference in milliseconds
			long diffDays = diff / (24 * 60 * 60 * 1000);             //in Days

			return diffDays;

		}

		/**
		*	Method to extract hour from the received time.
		*  @param   String strTime
		*  @return  int    hour 
		*/

		private static int getHour(String strTime){

			return strTime.length() == 4? Integer.parseInt(strTime.substring(0, 2)) : 0;
		}

		/**
		*	Method to extract minutes from the received time.
		*  @param   String strTime
		*  @return  int    minutes 
		*/

		private static int getMin(String strTime){

			return strTime.length() == 4? Integer.parseInt(strTime.substring(2, 4)) : 0;
		}

		/**
		*	Method to calculate layover if flights are on same day.
		*  @param   int depTime
		*  @param	int arrTime
		*  @return  int difference between departure and arrival time.  
		*/
		private static int getLayover(int depTime, int arrTime){

			return Math.abs(depTime - arrTime);

		}

		
		/**
		*	Method to calculate layover if flights are NOT on same day.
		*  @param   int arrHour, arrMin, depHour, depMin
		*  @return  int layover in minutes. 
		*/
		private static int _getLayover(int arrHour, int arrMin, int depHour, int depMin){

			int _arrHour = Math.abs(24-arrHour);
			int _hour = _arrHour + depHour;
			int _min = Math.abs(depMin - arrMin);
			int _layover = _hour * 60 - _min;

			return _layover;

		}

	}

	/**
	 *	Driver Program to run the jobs and set the input and output paths.
	 */
	public int run (String[] args) throws Exception {

		Job job = Job.getInstance();

		job.setJar("MissedConnection.jar");
		job.setMapperClass(ConnectionMapper.class);
		job.setReducerClass(ConnectionReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		System.exit(ToolRunner.run(new MissedConnection(), args));
	}
}

