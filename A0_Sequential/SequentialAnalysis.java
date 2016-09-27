import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.MultiValueMap;

/**
 * The SequentialAnalysis class performs analysis on a dataset that has information about flights in the USA.
 * The program displays a list of Carriers in ascending order of Average Ticket prices.
 * @author Harshali
 * @version 1/15/2016
 */

class SequentialAnalysis {

	/**
	 * This is the main method where file is read and parsed
	 * 
	 * @param   filename      Name of the gzipped file
	 * @throws  IOException
	 * 
	 */
	public static void main(String[] args) throws IOException {

		String FILENAME = "323.csv.gz";

		//Read the gzipped file
		InputStream fileStream = new FileInputStream(FILENAME);
		InputStream gzipStream = new GZIPInputStream(fileStream);
		Reader reader = new InputStreamReader(gzipStream, "UTF-8");

		//Use CSVParser (Apache Commons CSV) to read the csv file
		CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withHeader()); 

		//Sanity Test
		sanityCheck(parser);

		//Close Parser
		parser.close();

	}

	/**
	 * This method performs the Sanity test for each record in the CSV File
	 * CRSArrTime and CRSDepTime should not be zero
	 * timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime;
	 * timeZone % 60 should be 0
	 * AirportID,  AirportSeqID, CityMarketID, StateFips, Wac should be larger than 0
	 * Origin, Destination,  CityName, State, StateName should not be empty
	 * For flights that not Cancelled:
	 * ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
	 * if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
	 * if ArrDelay < 0 then ArrDelayMinutes should be zero
	 * if ArrDelayMinutes >= 15 then ArrDel15 should be true
	 * 
	 * @param parser    a CSV file
	 * 
	 */

	private static void sanityCheck(CSVParser parser) {

		int saneFlightCount = 0;           //set count of sane flights to 0
		int corruptFlightCount = 0;        //set count of corrupt flights to 0

		MultiMap saneFlights = new MultiValueMap();

		//Iterate through flight records
		for (CSVRecord record : parser) {

			boolean sane = false;         
			String carrierCode = record.get("CARRIER");
			Double avgTicketPrice = isDoubleParsable(record.get("AVG_TICKET_PRICE")) ? Double.parseDouble(record.get("AVG_TICKET_PRICE")) : 0.0;


			//Firstly, check if the field is parsable, if yes, call time conversion function, else set to 0
			int CRSArrTime = isIntParsable(record.get("CRS_ARR_TIME")) ? convertTime(record.get("CRS_ARR_TIME")) : 0;
			int CRSDepTime = isIntParsable(record.get("CRS_DEP_TIME")) ? convertTime(record.get("CRS_DEP_TIME")) : 0 ;
			int CRSElapsedTime = isIntParsable(record.get("CRS_ELAPSED_TIME")) ? Integer.parseInt(record.get("CRS_ELAPSED_TIME")) : 0 ;

			int timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime;

			//Check if the field is parsable, if yes, parse the field, else set to 0
			int OriginAirportID = isIntParsable(record.get("ORIGIN_AIRPORT_ID")) ? Integer.parseInt(record.get("ORIGIN_AIRPORT_ID")) : 0;
			int DestAirportID = isIntParsable(record.get("DEST_AIRPORT_ID")) ? Integer.parseInt(record.get("DEST_AIRPORT_ID")) : 0;

			int OriginAirportSeqID = isIntParsable(record.get("ORIGIN_AIRPORT_SEQ_ID")) ? Integer.parseInt(record.get("ORIGIN_AIRPORT_SEQ_ID")) : 0;
			int DestAirportSeqID = isIntParsable(record.get("DEST_AIRPORT_SEQ_ID")) ? Integer.parseInt(record.get("DEST_AIRPORT_SEQ_ID")) : 0;

			int OriginCityMarketID = isIntParsable(record.get("ORIGIN_CITY_MARKET_ID")) ? Integer.parseInt(record.get("ORIGIN_CITY_MARKET_ID")) : 0;
			int DestCityMarketID = isIntParsable(record.get("DEST_CITY_MARKET_ID")) ? Integer.parseInt(record.get("DEST_CITY_MARKET_ID")) : 0;

			int OriginStateFips = isIntParsable(record.get("ORIGIN_STATE_FIPS")) ? Integer.parseInt(record.get("ORIGIN_STATE_FIPS")) : 0;
			int DestStateFips = isIntParsable(record.get("DEST_STATE_FIPS")) ? Integer.parseInt(record.get("DEST_STATE_FIPS")) : 0;

			int OriginWac = isIntParsable(record.get("ORIGIN_WAC")) ? Integer.parseInt(record.get("ORIGIN_WAC")) : 0;
			int DestWac = isIntParsable(record.get("DEST_WAC")) ? Integer.parseInt(record.get("DEST_WAC")) : 0;

			//Check if the field is String, if yes, assign, else set to empty string
			String Origin = isAlpha(record.get("ORIGIN")) ? record.get("ORIGIN") : "";
			String Dest = isAlpha(record.get("DEST")) ? record.get("DEST") : "";

			String OriginCityName = isAlpha(record.get("ORIGIN_CITY_NAME")) ? record.get("ORIGIN_CITY_NAME") : "";
			String DestCityName = isAlpha(record.get("DEST_CITY_NAME")) ? record.get("DEST_CITY_NAME") : "";

			String OriginState = isAlpha(record.get("ORIGIN_STATE_ABR")) ? record.get("ORIGIN_STATE_ABR") : "";
			String DestState = isAlpha(record.get("DEST_STATE_ABR")) ? record.get("DEST_STATE_ABR") : "";

			String OriginStateName = isAlpha(record.get("ORIGIN_STATE_NM")) ? record.get("ORIGIN_STATE_NM") : "";
			String DestStateName = isAlpha(record.get("DEST_STATE_NM")) ? record.get("DEST_STATE_NM") : "";

			int Cancelled = isIntParsable(record.get("CANCELLED")) ? Integer.parseInt(record.get("CANCELLED")) : 1;

			int ArrTime = isIntParsable(record.get("ARR_TIME")) ? convertTime(record.get("ARR_TIME")) : 0;
			int DepTime = isIntParsable(record.get("DEP_TIME")) ? convertTime(record.get("DEP_TIME")) : 0;
			int ActualElapsedTime = isIntParsable(record.get("ACTUAL_ELAPSED_TIME")) ? Integer.parseInt(record.get("ACTUAL_ELAPSED_TIME")) : 0;

			Double ArrDelay = isDoubleParsable(record.get("ARR_DELAY")) ? Double.parseDouble(record.get("ARR_DELAY")) : 0;
			Double ArrDelayMinutes = isDoubleParsable(record.get("ARR_DELAY_NEW")) ? Double.parseDouble(record.get("ARR_DELAY_NEW")) : 0;
			Double ArrDel15 = isDoubleParsable(record.get("ARR_DEL15")) ? Double.parseDouble(record.get("ARR_DEL15")) : 0;


			//CRSArrTime and CRSDepTime should not be zero
			//timeZone % 60 should be 0

			if(CRSArrTime != 0 && CRSDepTime != 0 && timeZone % 60 == 0){

				//AirportID,  AirportSeqID, CityMarketID, StateFips, Wac should be larger than 0
				if(OriginAirportID > 0 && DestAirportID > 0 && OriginAirportSeqID > 0 && DestAirportSeqID > 0
						&& OriginCityMarketID > 0 && DestCityMarketID > 0 && OriginStateFips > 0 && DestStateFips > 0
						&& OriginWac > 0 && DestWac > 0){

					//Origin, Destination,  CityName, State, StateName should not be empty
					if(!isNullOrEmpty(Origin) && !isNullOrEmpty(Dest) 
							&& !isNullOrEmpty(OriginCityName) && !isNullOrEmpty(DestCityName)
							&& !isNullOrEmpty(OriginState) && !isNullOrEmpty(DestState)
							&& !isNullOrEmpty(OriginStateName) && !isNullOrEmpty(OriginStateName)){

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

			if(sane){
				saneFlightCount++;

				//Insert Carrier Code along with Average Price Tickets
				saneFlights.put(carrierCode, avgTicketPrice);

			}else{
				corruptFlightCount++;
			}
		}

		reduceFlightPrice(saneFlights, saneFlightCount, corruptFlightCount);

	}

	/**
	 * This method takes the Mean of the Average Ticket Prices of each Carrier Code 
	 * 
	 * @param saneFlights      a MultiValueMap containing a key-value pair of the Carrier Code and List of Average Ticket Prices
	 *                         e.g. <UA, [791.34, 715.11, 813.12, 961.95, 824.01,...]>
	 * @param saneCount        number of flights which passed the sanity test
	 * @param corruptCount     number of flights which failed the sanity test
	 * 
	 */
	private static void reduceFlightPrice(MultiMap saneFlights, int saneCount, int corruptCount){

		Map<Object, Double> saneFlightPrice = new HashMap<Object, Double>();

		Set<String> keys = saneFlights.keySet();
		for (Object k : keys){

			ArrayList<Double> priceList = (ArrayList)saneFlights.get(k);
			Double average = priceList.stream().mapToDouble(val -> val).average().getAsDouble();

			saneFlightPrice.put(k, average);
		}

		System.out.println(corruptCount);
		System.out.println(saneCount);

		Map<Object, Double> sortedMap = sortByValue(saneFlightPrice);

		for(Object key : sortedMap.keySet()) {
			System.out.println(key + " " + sortedMap.get(key));
		}

	}

	/******Helper Functions*******/
	
	/**
	 * This method checks if the String is Null or Empty
	 * @param  s        a String
	 * @return boolean  true if the string is null or empty, else false
	 */

	private static boolean isNullOrEmpty(String s) {
		return (s==null || s.length() == 0 || s.trim().equals(""));
	}
	
	/**
	 * This method checks if the input is Parsable as an Integer
	 * 
	 * @param  s          a String
	 * @return boolean    true if input is converted to an int , false if the string can't be converted to an int type.
	 * @throws NumberFormatException
	 */
	private static boolean isIntParsable(String input){
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
	private static boolean isDoubleParsable(String input){
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
	private static boolean isAlpha(String s){
		return s.trim().matches("[^0-9]+");	
	}

	/**
	 * This method converts the given input from hhmm format to minutes
	 * 
	 * @param  s          a String
	 * @return time       number of minutes calculated by converting the time from hhmm format
	 */
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
