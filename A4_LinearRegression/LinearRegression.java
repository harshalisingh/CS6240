// Author: Vishal Mehta, Harshali Singh

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

	/*
	 * LinearRegression -- The map reduce program that generates the list of airlines with average price 
	 * in year 2010-2014 and only for those airlines which are active in 2015.
	 */
public class LinearRegression extends Configured implements Tool {

	/*
	 * Mapper class to read the entire data and filter out the flights which
	 * are active in the year 2010 - 2015 and passes the key value pair to the
	 * reduce program. Key is the carrier code and Value is the combination of Avg. ticket
	 * price, year , distance and flight time for that particular airline.
	 * 
	 */
	
	public static class LinearMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			//Split the record
			String line = value.toString();
			String newLine = line.replaceAll("\"", "");
			String formattedLine = newLine.replaceAll(", ", ":");
			String[] row = formattedLine.split(",");

			//Check if the flight passed the sanity test.
			if (sanityTest(row)) {
			
				String carrierCode = row[6];
				String avgTicketPrice = row[109];
				int year = Integer.parseInt(row[0]);
				String distance = row[54];
				String flightTime = row[51];


					Text outKey = null;
					Text outValue = null;

					outKey = createKey(carrierCode);
					outValue = createValue(year, avgTicketPrice, distance, flightTime);

					context.write(outKey, outValue);
					

			}

		}

		/* Method to Create Mapper output Key
		 */
		private Text createKey(String carrier) {
			Text returnKey = null;
			if (!carrier.isEmpty()) {
				returnKey = new Text(carrier.trim());
			}
			return returnKey;
		}
		
		/* Method to Create Mapper output Value
		 */
		private Text createValue(int year, String avgPrice, String distance, String flight) {
			Text returnValue  = new Text(String.valueOf(year) + "\t" + avgPrice + "\t" + distance + "\t" + flight);

			return returnValue;
		}
		
		/*
			Method to enforce the sanity test for every row (flight data).
			Returns: Boolean value depending on whether a flight passes
			sanity test or not.
		*/
		private static boolean sanityTest(String[] row){

			//System.out.println(row.length);

			if(row.length != 110){
				return false;
			}

			try {

				int year = Integer.parseInt(row[0]);

				if(year < 2010 || year > 2015 ){
					return false;
				}

				//Time is converted to minutes
				int CRSArrTime = row[40].isEmpty()?0:convertTime(row[40]);
				int CRSDepTime = row[29].isEmpty()?0:convertTime(row[29]);
				int CRSElapsedTime = (int) Float.parseFloat(row[50]);
				int timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime;

				//CRSArrTime and CRSDepTime should not be zero

				if (CRSArrTime == 0 || CRSDepTime == 0) {
					return false;
				}	

				//timeZone % 60 should be 0
				if (timeZone % 60 != 0) {
					return false;
				}

				int OriginAirportId = (int) Float.parseFloat(row[11]);
				int DestAirportId = (int) Float.parseFloat(row[20]);
				int OriginAirportSeqId = (int) Float.parseFloat(row[12]);
				int DestAirportSeqId = (int) Float.parseFloat(row[21]);
				int OriginCityMarketId = (int) Float.parseFloat(row[13]);
				int DestCityMarketId = (int) Float.parseFloat(row[22]);
				int OriginStateFips = (int) Float.parseFloat(row[17]);
				int DestStateFips = (int) Float.parseFloat(row[26]);
				int OriginWac = (int) Float.parseFloat(row[19]);
				int DestWac = (int) Float.parseFloat(row[28]);

				//AirportID,  AirportSeqID, CityMarketID, StateFips, Wac should be larger than 0
				if (OriginAirportId <= 0 || DestAirportId <= 0 || OriginAirportSeqId <= 0 || 
						DestAirportSeqId <= 0 || OriginCityMarketId <= 0 || DestCityMarketId <= 0 || 
						OriginStateFips <= 0 || DestStateFips <= 0 || OriginWac <= 0 || DestWac <= 0) {
					return false;
				}

				//Origin, Destination,  CityName, State, StateName should not be empty
				if (row[14].isEmpty() || row[23].isEmpty() || row[15].isEmpty() || row[24].isEmpty() ||
						row[16].isEmpty() || row[25].isEmpty() || row[18].isEmpty() || row[27].isEmpty()) {
					return false;
				}

				//Flights that are not Cancelled
				int cancelled = (int) Float.parseFloat(row[47]);
				if (cancelled != 1) {
					int arrTime = row[41].isEmpty()?0:convertTime(row[41]);
					int depTime = row[30].isEmpty()?0:convertTime(row[30]);
					int actualElapsedTime = (int) Float.parseFloat(row[51]);

					//ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
					int diff = arrTime - depTime - actualElapsedTime - timeZone;
					if (diff != 0 && diff % 1440 != 0) {
						return false;
					}

					int arrDelay = (int) Float.parseFloat(row[42]);
					int arrDelayMinutes = (int) Float.parseFloat(row[43]);

					//if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
					//if ArrDelay < 0 then ArrDelayMinutes should be zero
					if (arrDelay > 0 && arrDelay != arrDelayMinutes) {
						return false;
					} else if (arrDelay < 0 && arrDelayMinutes != 0) {
						return false;
					}

					//if ArrDelayMinutes >= 15 then ArrDel15 should be true
					if (arrDelayMinutes >= 15 && ((int) Float.parseFloat(row[44])) != 1) {
						return false;
					}
				}

			} catch (NumberFormatException exception) {

				return false;

			}

			return true;		

		}

		/*Method to Convert time in hh:mm format to Minutes.*/
		private static int convertTime(String s){

			String result = "";
			int time = 0;
			String s_new = s.trim();
			if(s_new.length()< 4){
				result = "0" + s_new;
			}
			else {
				result = s_new;
			}

			if(result.length() == 4){
				int hours = Integer.parseInt(result.substring(0,2));
				int minutes = Integer.parseInt(result.substring(2,4));

				time = hours * 60 + minutes;

			}
			return time;

		}


	}

	/*
	 * Reducer class to emit the avg. ticket prices for each
	 * airline in the year 2010-2014 and which are also active in 2015. It takes the key-value pair from the mapper.
	 * Emits the key value pair, key is the carrier code and Value is the avg. ticket prices for that
	 * airline.
	 * 
	 */
	public static class LinearReducer extends Reducer<Text, Text, Text, Text>{

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException, IndexOutOfBoundsException {

			boolean active2015 = false;
			
			// Check if that airline is active in 2015.
			for(Text value: values){
				
				String[] split = value.toString().split("\t");
				int year = Integer.parseInt(split[0]);
				
				if(year == 2015){
					active2015 = true;
					break;
				}
				
			}
			
			// Only output 2010-2014 records
			if(active2015){
				
				for(Text value: values){
					
					String[] split = value.toString().split("\t");
					int year = Integer.parseInt(split[0]);
					
					if(year != 2015){
						
						context.write(key, new Text(String.valueOf(value)));
						
					}
					
				}
				
			}

		}

	}

	/*
		Driver Program to run the jobs and set the input and output paths.
	*/
	public int run (String[] args) throws Exception {

		Job job = Job.getInstance();

		job.setJar("LinearRegression.jar");
		job.setMapperClass(LinearMapper.class);
		job.setReducerClass(LinearReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		System.exit(ToolRunner.run(new LinearRegression(), args));
	}
}
