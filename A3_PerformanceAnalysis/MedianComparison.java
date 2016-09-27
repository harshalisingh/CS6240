//Author : Vishal Mehta,Harshali Singh

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;


	/*
	 * Mapper program to read the entire data and filter out the flights which
	 * are not active in the year 2015 and passes the key value pair to the
	 * reduce program. Key is the carrier code and Value is the Avg. ticket
	 * price for that particular airline.
	 * 
	 */
public class MedianComparison extends Configured {

	public static class MedianMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String newLine = line.replaceAll("\"", "");
			String formattedLine = newLine.replaceAll(", ", ":");
			String[] row = formattedLine.split(",");

			String carrierCode = null;
			String avgTicketPrice = null;
			String month = null;
			int year = 0;

			if (sanityTest(row)) {
		
				carrierCode = row[6];
				avgTicketPrice = row[109];
				month = row[2];
				year = Integer.parseInt(row[0]);
				
				if(year == 2015){
					
					Text outKey = null;
					Text outValue = null;

					outKey = createKey(month, carrierCode);
					outValue = createValue(avgTicketPrice);

					context.write(outKey, outValue);
					
				}

			}

		}

		private Text createKey(String month, String carrier) {
			Text returnKey = null;
			if (!carrier.isEmpty() && !month.isEmpty()) {
				returnKey = new Text(month.trim() + "\t" + carrier.trim());
			}
			return returnKey;
		}

		private Text createValue(String avgPrice) {
			Text returnValue = null;
			if (!avgPrice.isEmpty()) {
				returnValue = new Text(avgPrice);
			}
			return returnValue;
		}


		private static boolean sanityTest(String[] row){

			//System.out.println(row.length);

			if(row.length != 110){
				return false;
			}

			try {

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
		// Convert time to Minutes.
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
	 * Reducer program to emit the consolidated avg. ticket prices for each
	 * airline in the year 2015. It takes the key value pair from the mapper.
	 * key is the carrier code and Value is the median prices for the
	 * airline.
	 * 
	 */
	public static class MedianReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException, IndexOutOfBoundsException {


			ArrayList<Float> prices = new ArrayList<Float>();
			int count = 0;
			for (Text value : values) {
				// System.out.println("Inside " +value.toString());
				prices.add(Float.parseFloat(value.toString()));
				count = count + 1;

			}
			
			Collections.sort(prices);
			int priceSize  = prices.size();
			
			float medianPrice;

			if(priceSize % 2 == 0){

				medianPrice  = (prices.get(priceSize / 2) + prices.get((priceSize / 2) - 1)) / 2;
				
			} else {
				
				medianPrice = prices.get(prices.size() / 2 - 1);
			}

			context.write(key, new Text(count + "\t" + String.valueOf(medianPrice)));

		}
	}


	//Driver Program
	public static void main(String args[]) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: MedianComparison <input-path> <output-path>");
		}
		final long startTime = System.currentTimeMillis();

		Job job = Job.getInstance();

		// job.getConfiguration().set("join.type", joinType);
		job.setJar("MedianComparison.jar");
		job.setMapperClass(MedianMapper.class);
		job.setReducerClass(MedianReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		long duration = System.currentTimeMillis() - startTime;
	
		job.waitForCompletion(true);

		FileWriter writer = new FileWriter("timemedian.txt", true);
		BufferedWriter bufferWritter = new BufferedWriter(writer);
		// bufferWritter.write("check,blah\n");
		bufferWritter.write(String.valueOf(duration));
		bufferWritter.close();
		System.out.println(duration);
		
	}

}

