

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

/**
 * The MultithreadedAnalysis class performs multithreaded analysis on Two years worth of dataset that has information about flights in the USA.
 * The program displays a list of Carriers in ascending order of Average Ticket prices.
 * @ author: Harshali Singh, Vishal Mehta
 * @version 1/19/2016
 */

public class MedianSingle {

	/**
	 * This is the main method where file is read and parsed
	 * 
	 * @param   filename      Name of the gzipped file
	 * @throws  IOException
	 * 
	 */

	private static final Object LOCK = new Object();

	private static Integer F = 0;          //F set count of sane flights to 0
	private static Integer K = 0;         //K set count of corrupt flights to 0

	private static Map<String, Float> resultMap = new HashMap<String, Float>();

	private static Map<String, ArrayList<Float>> carrierPriceList = new HashMap<String, ArrayList<Float>>();
	private static Map<String,Float> carrierFrequency = new HashMap<String,Float>();

	public static void main(String[] args) throws IOException, InterruptedException {

		if (args.length != 1) {

			System.out.println("Usage (Single-Threaded): <java> -input=DIR");

			return;
		}

		final long startTime = System.currentTimeMillis();
// Author: Harshali Singh, Vishal Mehta
		//Sequential
		String[] split = args[0].split("=");
		String directory = split[1];

		File dir = new File(directory);
		File[] files = dir.listFiles();

		MedianSingle ms = new MedianSingle();

		if(args.length == 1){
			System.out.println("Running Median Single Threaded");
			for (File f : files) {
				WorkerThread thread = ms.new WorkerThread(f);
				thread.run();
			}
		}


		System.out.println(K);
		System.out.println(F);

		resultMap = computeMedian(carrierPriceList);

		//outputMap stores sorted mean average price for each carrier
		TreeMap<String, Float> sortedMap = sortValues(carrierFrequency);

		printResult(resultMap, sortedMap);


		final long duration = System.currentTimeMillis() - startTime;
		System.out.println(duration);
	}

	private static TreeMap<String, Float> sortValues(Map<String, Float> carrierFrequency2) {
		// TODO Auto-generated method stub
		MedianSingle ms = new MedianSingle();
		SortValues sc = ms.new SortValues(carrierFrequency2);
		TreeMap<String, Float> map= new TreeMap<>(sc);
		map.putAll(carrierFrequency2);
		return map;
	}

	private static void printResult(Map<String, Float> map, Map<String, Float> sorted){

		for (Map.Entry<String, Float> entry : sorted.entrySet()) {
			String key = entry.getKey();
			System.out.println(key + "\t" +  map.get(key));
		}

	}


	@SuppressWarnings("unused")
	private static Map<String, Float> computeMedian(Map<String, ArrayList<Float>> carrierPriceList) {
		Map<String, Float> median = new HashMap<String, Float>();

		for (Entry<String, ArrayList<Float>> entry : carrierPriceList.entrySet()) {
			String carrier = entry.getKey();
			List<Float> prices = entry.getValue();
			Collections.sort(prices);
			float medianPrice;
			int priceSize = prices.size(); 
			if (priceSize % 2 == 0) {
				medianPrice = (prices.get(priceSize / 2) + prices.get(priceSize / 2 - 1)) / 2;
			} else {
				medianPrice = prices.get(prices.size() / 2 - 1);
			}

			median.put(carrier, medianPrice);
		}

		return median;
	}

	class WorkerThread extends Thread {

		final File file;	

		int saneFlightCount = 0;          //F set count of sane flights to 0
		int corruptFlightCount = 0;       //K set count of corrupt flights to 0

		Map<String, ArrayList<Float>> cPriceList = new HashMap<String, ArrayList<Float>>();
		Map<String,Float> cFrequency = new HashMap<String,Float>();

		WorkerThread(File file){
			this.file = file;
		}

		@Override
		public void run() {

			//System.out.println("File: " + file);
			InputStream fileStream;
			try {
				fileStream = new FileInputStream(file);
				InputStream gzipStream = new GZIPInputStream(fileStream);
				@SuppressWarnings("resource")
				BufferedReader br = new BufferedReader(new InputStreamReader(gzipStream));

				String current = null;
				while((current = br.readLine()) != null){

					String[] row = parseFlight(current);

					//System.out.println("Row: " + row);

					if(sanityTest(row)){

						//System.out.println("true");

						saneFlightCount++;    //F of a particular file

						String carrierCode = row[8];
						Float avgTicketPrice = Float.parseFloat(row[109]);

						int year = (int) Float.parseFloat(row[0]);
						int month = (int) Float.parseFloat(row[2]);

						updateCarrier(carrierCode, avgTicketPrice);


						if(year == 2015){

							updateCarrier(month + "\t" + carrierCode, avgTicketPrice);

						}

					}
					else{

						//System.out.println("false" + row.length);

						corruptFlightCount++;   //K of a particular file

					}


				}

				synchronized (LOCK) {
					MedianSingle.K += corruptFlightCount;
					MedianSingle.F += saneFlightCount;
				}

				synchronized (MedianSingle.carrierPriceList) {
					updatePriceList(MedianSingle.carrierPriceList, cPriceList);
				}

				synchronized (MedianSingle.carrierFrequency) {
					updateFrequency(MedianSingle.carrierFrequency, cFrequency);
				}



			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		private void updateCarrier(String carrierCode, Float avgTicketPrice){

			ArrayList<Float> priceList = cPriceList.get(carrierCode);
			if (priceList == null) {
				ArrayList<Float> list = new ArrayList<Float>();
				list.add(avgTicketPrice);
				cPriceList.put(carrierCode, list);
				cFrequency.put(carrierCode, (float) 1.0);
			} else {
				priceList.add(avgTicketPrice);
				cFrequency.put(carrierCode, cFrequency.get(carrierCode) + 1);
			}

		}

		private void updatePriceList(Map<String, ArrayList<Float>> globalList, Map<String, ArrayList<Float>> localList){

			for (Entry<String, ArrayList<Float>> entry : localList.entrySet()) {
				String carrier = entry.getKey();
				ArrayList<Float> localPriceList = entry.getValue();
				List<Float> globalPriceList = globalList.get(carrier);
				if (globalPriceList == null) {
					globalList.put(carrier, localPriceList);
				} else {
					globalPriceList.addAll(localPriceList);
				}				
			}
		}


		private void updateFrequency(Map<String, Float> globalList, Map<String, Float> localList){

			for (Entry<String, Float> entry : localList.entrySet()) {
				String carrier = entry.getKey();
				Float localFrequency = entry.getValue();
				Float globalFrequency = globalList.get(carrier);
				if (globalFrequency == null) {
					globalList.put(carrier, localFrequency);
				} else {
					globalList.put(carrier, globalList.get(carrier)+localFrequency);
				}				
			}
		}


public String[] parseFlight(String line){
			
			List<String> columns = new ArrayList<String>();
			StringBuffer sb = new StringBuffer();
			boolean qStart = false;
			char cur;
			//System.out.println(line);
			for (int i = 0; i < line.length(); i++) {
				cur = line.charAt(i);
				if (qStart) {
					if (cur == '"') {
						qStart= false;
					} else {
						sb.append(cur);
					}
				} 
				else {
					if (cur == ',') {
						columns.add(sb.toString());
						sb = new StringBuffer();
					} else if (cur == '"') {
						qStart = true;
					} else {
						sb.append(cur);
					}
				}
			}
			columns.add(sb.toString());

			return columns.toArray(new String[1]);
			
		}
		
		
		public boolean sanityTest(String[] row){
			
			//System.out.println(row.length);
			
			if(row.length != 110){
				return false;
			}
			
			try {
				
				//String carrierCode = row[8];
				//System.out.println(carrierCode);
				
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
		
		public int convertTime(String s){

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


	class SortValues implements Comparator<String> {
		Map<String, Float> h;

		public SortValues(Map<String, Float> carrierFrequency2) {
			// TODO Auto-generated constructor stub
			this.h = carrierFrequency2;
		}

		@Override
		public int compare(String arg1, String arg2) {
			// TODO Auto-generated method stub
			if (h.get(arg1) > h.get(arg2))
				return -1;
			return 1;
		}

	}

}

