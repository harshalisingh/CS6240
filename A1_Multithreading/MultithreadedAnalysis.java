package com.mapreduce.threaded;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.zip.GZIPInputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.MultiValueMap;

/**
 * The MultithreadedAnalysis class performs multithreaded analysis on Two years worth of dataset that has information about flights in the USA.
 * The program displays a list of Carriers in ascending order of Average Ticket prices.
 * @author Harshali
 * @version 1/19/2016
 */

class MultithreadedAnalysis {

	/**
	 * This is the main method where file is read and parsed
	 * 
	 * @param   filename      Name of the gzipped file
	 * @throws  IOException
	 * 
	 */

	public static Integer F = 0;          //F set count of sane flights to 0
	public static Integer K = 0;       //K set count of corrupt flights to 0

	public static MultiMap saneFlights = new MultiValueMap();


	public static void main(String[] args) throws IOException, InterruptedException {
		
		if(args.length != 1 || args.length != 2){
			System.out.println("Enter Valid Arguments");
			System.exit(0);
		}

		//final long startTime = System.currentTimeMillis();

		String directory = null;
		if(args.length == 2){
			
			String[] split = args[1].split("=");
			directory = split[1];
		}
		
		if(args.length == 1){
			String[] split = args[0].split("=");
			directory = split[1];
		}
		
		
		String filePath = directory;
		File dir = new File(filePath);
		File[] files = dir.listFiles();

		int numThreads = 0;
		if(args.length == 2){
			numThreads = files.length;
		}
		else{
			numThreads = 1;
		}

		//System.out.println("Threads: " + numThreads);
		WorkerThread[] threads = new WorkerThread[numThreads];

		MultithreadedAnalysis ma = new MultithreadedAnalysis();

		int i = 0;
		for (File f : files) {
			if(f.isFile()) {

				threads[i] = ma.new WorkerThread(f);
				threads[i].start();

			}
			i++;
		}

		/*for(int t = 0; t < numThreads; t++){
			
		}*/

		for(int t = 0; t < numThreads; t++){
			threads[t].join();
		}
		
		
		for(int t = 0; t < numThreads; t++){
			F += threads[t].saneFlightCount;
			K += threads[t].corruptFlightCount;
			
		}

		reduceFlightPrice();

		/*final long duration = System.currentTimeMillis() - startTime;
		System.out.println("Duration: " + duration + "ms");*/
	}

	/**
	 * This method retrieves a priceList for a particular Carrier code and calculates the 
	 * mean price ticket and the median price ticket. 
	 * 
	 */

	private static void reduceFlightPrice(){

		Map<Object, Double> saneFlightMedian = new HashMap<Object, Double>();
		Map<Object, Double> saneFlightMean = new HashMap<Object, Double>();


		Set<String> keys = saneFlights.keySet();
		for (Object k : keys){

			ArrayList<Double> priceList = (ArrayList)saneFlights.get(k);
			Double mean = priceList.stream().mapToDouble(val -> val).average().getAsDouble();
			Double median = Helper.getMedian(priceList);

			saneFlightMedian.put(k, median);
			saneFlightMean.put(k, mean);
		}

		System.out.println(F);
		System.out.println(K);

		Map<Object, Double> sortedMapMedian = Helper.sortByValue(saneFlightMedian);

		for(Object key : sortedMapMedian.keySet()) {
			System.out.println(key + " " + saneFlightMean.get(key) + " " + sortedMapMedian.get(key));
		}

	}


	class WorkerThread extends Thread {

		final File file;	
		int saneFlightCount;          //F set count of sane flights to 0
		int corruptFlightCount;       //K set count of corrupt flights to 0

		MultiMap saneThread = new MultiValueMap();
		
		WorkerThread(File file){
			saneFlightCount = 0;
			corruptFlightCount = 0;
			this.file = file;
		}

		@Override
		public void run() {

			//System.out.println("File: " + f);
			InputStream fileStream;
			CSVParser parser = null;
			try {
				fileStream = new FileInputStream(file);
				InputStream gzipStream = new GZIPInputStream(fileStream);
				Reader reader = new InputStreamReader(gzipStream, "UTF-8");

				//Use CSVParser (Apache Commons CSV) to read the csv file
				parser = new CSVParser(reader, CSVFormat.EXCEL.withHeader());

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}


			//Iterate through flight records
			for (CSVRecord record : parser) {

				if (record.size() != 110) {
					corruptFlightCount++;
					continue;
				}
				boolean sane = false;

				String carrierCode = record.get("CARRIER");
				Double avgTicketPrice = 0.0;

				int Year = Helper.isIntParsable(record.get("YEAR")) ? Integer.parseInt(record.get("YEAR")) : 0;
				int Month = Helper.isIntParsable(record.get("MONTH")) ? Integer.parseInt(record.get("MONTH")) : 0;

				//Firstly, check if the field is parsable, if yes, call time conversion function, else set to 0
				int CRSArrTime = Helper.isIntParsable(record.get("CRS_ARR_TIME")) ? Helper.convertTime(record.get("CRS_ARR_TIME")) : 0;
				int CRSDepTime = Helper.isIntParsable(record.get("CRS_DEP_TIME")) ? Helper.convertTime(record.get("CRS_DEP_TIME")) : 0 ;
				int CRSElapsedTime = Helper.isIntParsable(record.get("CRS_ELAPSED_TIME")) ? Integer.parseInt(record.get("CRS_ELAPSED_TIME")) : 0 ;

				int timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime;

				//Check if the field is parsable, if yes, parse the field, else set to 0
				int OriginAirportID = Helper.isIntParsable(record.get("ORIGIN_AIRPORT_ID")) ? Integer.parseInt(record.get("ORIGIN_AIRPORT_ID")) : 0;
				int DestAirportID = Helper.isIntParsable(record.get("DEST_AIRPORT_ID")) ? Integer.parseInt(record.get("DEST_AIRPORT_ID")) : 0;

				int OriginAirportSeqID = Helper.isIntParsable(record.get("ORIGIN_AIRPORT_SEQ_ID")) ? Integer.parseInt(record.get("ORIGIN_AIRPORT_SEQ_ID")) : 0;
				int DestAirportSeqID = Helper.isIntParsable(record.get("DEST_AIRPORT_SEQ_ID")) ? Integer.parseInt(record.get("DEST_AIRPORT_SEQ_ID")) : 0;

				int OriginCityMarketID = Helper.isIntParsable(record.get("ORIGIN_CITY_MARKET_ID")) ? Integer.parseInt(record.get("ORIGIN_CITY_MARKET_ID")) : 0;
				int DestCityMarketID = Helper.isIntParsable(record.get("DEST_CITY_MARKET_ID")) ? Integer.parseInt(record.get("DEST_CITY_MARKET_ID")) : 0;

				int OriginStateFips = Helper.isIntParsable(record.get("ORIGIN_STATE_FIPS")) ? Integer.parseInt(record.get("ORIGIN_STATE_FIPS")) : 0;
				int DestStateFips = Helper.isIntParsable(record.get("DEST_STATE_FIPS")) ? Integer.parseInt(record.get("DEST_STATE_FIPS")) : 0;

				int OriginWac = Helper.isIntParsable(record.get("ORIGIN_WAC")) ? Integer.parseInt(record.get("ORIGIN_WAC")) : 0;
				int DestWac = Helper.isIntParsable(record.get("DEST_WAC")) ? Integer.parseInt(record.get("DEST_WAC")) : 0;

				//Check if the field is String, if yes, assign, else set to empty string
				String Origin = Helper.isAlpha(record.get("ORIGIN")) ? record.get("ORIGIN") : "";
				String Dest = Helper.isAlpha(record.get("DEST")) ? record.get("DEST") : "";

				String OriginCityName = Helper.isAlpha(record.get("ORIGIN_CITY_NAME")) ? record.get("ORIGIN_CITY_NAME") : "";
				String DestCityName = Helper.isAlpha(record.get("DEST_CITY_NAME")) ? record.get("DEST_CITY_NAME") : "";

				String OriginState = Helper.isAlpha(record.get("ORIGIN_STATE_ABR")) ? record.get("ORIGIN_STATE_ABR") : "";
				String DestState = Helper.isAlpha(record.get("DEST_STATE_ABR")) ? record.get("DEST_STATE_ABR") : "";

				String OriginStateName = Helper.isAlpha(record.get("ORIGIN_STATE_NM")) ? record.get("ORIGIN_STATE_NM") : "";
				String DestStateName = Helper.isAlpha(record.get("DEST_STATE_NM")) ? record.get("DEST_STATE_NM") : "";

				int Cancelled = Helper.isIntParsable(record.get("CANCELLED")) ? Integer.parseInt(record.get("CANCELLED")) : 1;

				int ArrTime = Helper.isIntParsable(record.get("ARR_TIME")) ? Helper.convertTime(record.get("ARR_TIME")) : 0;
				int DepTime = Helper.isIntParsable(record.get("DEP_TIME")) ? Helper.convertTime(record.get("DEP_TIME")) : 0;
				int ActualElapsedTime = Helper.isIntParsable(record.get("ACTUAL_ELAPSED_TIME")) ? Integer.parseInt(record.get("ACTUAL_ELAPSED_TIME")) : 0;

				Double ArrDelay = Helper.isDoubleParsable(record.get("ARR_DELAY")) ? Double.parseDouble(record.get("ARR_DELAY")) : 0;
				Double ArrDelayMinutes = Helper.isDoubleParsable(record.get("ARR_DELAY_NEW")) ? Double.parseDouble(record.get("ARR_DELAY_NEW")) : 0;
				Double ArrDel15 = Helper.isDoubleParsable(record.get("ARR_DEL15")) ? Double.parseDouble(record.get("ARR_DEL15")) : 0;


				//CRSArrTime and CRSDepTime should not be zero
				//timeZone % 60 should be 0

				if(record.size() == 110){

					avgTicketPrice = Helper.isDoubleParsable(record.get("AVG_TICKET_PRICE")) ? Double.parseDouble(record.get("AVG_TICKET_PRICE")) : 0.0;

					if(CRSArrTime != 0 && CRSDepTime != 0 && timeZone % 60 == 0){

						//AirportID,  AirportSeqID, CityMarketID, StateFips, Wac should be larger than 0
						if(OriginAirportID > 0 && DestAirportID > 0 && OriginAirportSeqID > 0 && DestAirportSeqID > 0
				&& OriginCityMarketID > 0 && DestCityMarketID > 0 && OriginStateFips > 0 && DestStateFips > 0
				&& OriginWac > 0 && DestWac > 0){

							//Origin, Destination,  CityName, State, StateName should not be empty
							if(!Helper.isNullOrEmpty(Origin) && !Helper.isNullOrEmpty(Dest) 
									&& !Helper.isNullOrEmpty(OriginCityName) && !Helper.isNullOrEmpty(DestCityName)
									&& !Helper.isNullOrEmpty(OriginState) && !Helper.isNullOrEmpty(DestState)
									&& !Helper.isNullOrEmpty(OriginStateName) && !Helper.isNullOrEmpty(OriginStateName)){

								//Flights that are not Cancelled
								if(Cancelled != 1){

									int diff = ((ArrTime - DepTime - ActualElapsedTime - timeZone)/60)%24;

									//ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
									if(diff == 0){

										//if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
										//if ArrDelay < 0 then ArrDelayMinutes should be zero

										if(ArrDelay > 0 && ArrDelay == ArrDelayMinutes || (ArrDelay < 0 && ArrDelayMinutes == 0)) {
											sane = true;
										} else {
											sane = false;
										}

										//if ArrDelayMinutes >= 15 then ArrDel15 should be true
										if((ArrDelayMinutes >=15 && ArrDel15 == 1) || (ArrDelayMinutes < 15) ){
											sane = true;
										}

									}
									else{
										sane = false;
									}

								}
								else {
									sane = true;        //Flights that were Cancelled
								}

							} 


						}

					}

				}


				if(sane){

					saneFlightCount++;    //F of a particular file

					if(Year == 2015 && Month == 1){

							//Insert Carrier Code along with Average Price Tickets
							saneFlights.put(carrierCode, avgTicketPrice); 

					}



				}
				else{
					corruptFlightCount++;   //K of a particular file
				}

			}

		} 

	} 

}




/******Helper Functions*******/


class Helper{

	/**
	 * This method checks if the average ticket price is an outlier or not
	 * @param  avgTicketPrice     Double
	 * @return boolean            true if the string is null or empty, else false
	 */

	public static boolean isOutlier(Double avgTicketPrice){
		if((avgTicketPrice == 0) || (avgTicketPrice == 999999999) ){
			return true;
		}
		else return false;

	}

	/**
	 * This method checks if the String is Null or Empty
	 * @param  s        a String
	 * @return boolean  true if the string is null or empty, else false
	 */

	public static boolean isNullOrEmpty(String s) {
		return (s==null || s.length() == 0 || s.trim().equals(""));
	}

	/**
	 * This method checks if the input is Parsable as an Integer
	 * 
	 * @param  s          a String
	 * @return boolean    true if input is converted to an int , false if the string can't be converted to an int type.
	 * @throws NumberFormatException
	 */
	public static boolean isIntParsable(String input){
		boolean parsable = true;
		try{
			Integer.parseInt(input);
		}catch(NumberFormatException e){
			parsable = false;
		}
		return parsable;
	}

	/**
	 * This method checks if the input is Parsable as a Double
	 * 
	 * @param  s          a String
	 * @return boolean    true if input is converted to a Double , false if the string can't be converted to a Double type.
	 * @throws NumberFormatException
	 */
	public static boolean isDoubleParsable(String input){
		boolean parsable = true;
		try{
			Double.parseDouble(input);
		}catch(NumberFormatException e){
			parsable = false;
		}
		return parsable;
	}

	/**
	 * This method checks if the input is String
	 * 
	 * @param  s          a String
	 * @return boolean    true if input contains characters and a comma e.g. New York, NY is a valid string
	 */
	public static boolean isAlpha(String s){
		return s.trim().matches("[^0-9]+");	
	}

	/**
	 * This method converts the given input from hhmm format to minutes
	 * 
	 * @param  s          a String
	 * @return time       number of minutes calculated by converting the time from hhmm format
	 */
	public static int convertTime(String s){

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

	public static Double getMedian(ArrayList<Double> values){

		Collections.sort(values);

		if (values.size() % 2 == 1)
			return (Double) values.get((values.size()+1)/2-1);
		else
		{
			double lower = (double) values.get(values.size()/2-1);
			double upper = (double) values.get(values.size()/2);

			return (lower + upper) / 2.0;
		}	
	}

	/**
	 * This method Sorts a Map<Key, Value> by values 
	 * 
	 * @param  map         a Map<Key, Value> e.g. 
	 * @param  result      a Sorted Map<Key, Value>
	 * Reference: http://stackoverflow.com/questions/109383/sort-a-mapkey-value-by-values-java
	 * 
	 */

	public static <K, V extends Comparable<? super V>> Map<K, V> 
	sortByValue( Map<K, V> map )
	{
		List<Map.Entry<K, V>> list =
				new LinkedList<>( map.entrySet() );
		Collections.sort( list, new Comparator<Map.Entry<K, V>>()
		{
			@Override
			public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
			{
				return (o1.getValue()).compareTo( o2.getValue() );
			}
		} );

		Map<K, V> result = new LinkedHashMap<>();
		for (Map.Entry<K, V> entry : list)
		{
			result.put( entry.getKey(), entry.getValue() );
		}
		return result;
	}

}
