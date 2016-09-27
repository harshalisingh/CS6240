
/**
 * @author Harshali Singh
 *
 */

import java.text.ParseException;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class FlightUtils {
	
	/*
	Method to enforce the sanity test for every row (flight data).
	Returns: Boolean value depending on whether a flight passes
	sanity test or not.
	 */
	public static boolean sanityTest(String[] row){

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
	
	public static boolean sanityTest(String[] row, boolean isTest){
		
		//System.out.println(row.length + "*****************************Length");
		
		if(row.length != 112){
			
			return false;
		}
		
		try{
			
			Integer year = Integer.parseInt(row[1]);
			Integer month = Integer.parseInt(row[3]);
			
			Integer dayOfMonth = Integer.parseInt(row[4]);
			Integer dayOfWeek = Integer.parseInt(row[5]);
			String carrier = row[9];
			Integer flightNum = Integer.parseInt(row[11]);
			String flightDate = row[6];
			Integer originId = Integer.parseInt(row[12]);
			Integer destId = Integer.parseInt(row[21]);
			Integer crsDepartureTime = Integer.parseInt(row[30]);
			Integer crsArrivalTime = Integer.parseInt(row[41]);
			
			if(carrier == "NA" || flightDate == "NA"){
				
				return false;
				
			}
			
			
		} catch (NumberFormatException ex){

			return false;
		}
		
		return true;
	}

	/**
	* Method to convert time in hh:mm format to Minutes.
	* @param String s (time in hh:mm)
	* @return int    time in minutes
	*/
	public  static int convertTime(String s){

		String result = appendString(s);
		
		int time = 0;

		if(result.length() == 4){
			int hours = Integer.parseInt(result.substring(0,2));
			int minutes = Integer.parseInt(result.substring(2,4));

			time = hours * 60 + minutes;

		}
		return time;

	}
	
	/**
	* Method to append number of zeros to the time based on its length
	* @param String s (time in hh:mm)
	* @return String appended string with zeros.
	*/
	public static String appendString(String s){
		
		String result = "";
		String s_new = s.trim();
		if(s_new.length()< 4 && !s_new.isEmpty()){
			result = "0" + s_new;
		}
		else {
			result = s_new;
		}
		
		return result;
	}
	
	/**
	 * @param originalDate
	 * @param dateList
	 * Method takes the current date and the list of holidays and returns the days 
	 * left to the closest holiday
	 * */
	public static int closerDate(Date originalDate, List<Date> dateList) {
		Collections.sort(dateList);
		Iterator<Date> iterator = dateList.iterator();
		Date previousDate = null;
		while (iterator.hasNext()) {
			Date nextDate = iterator.next();
			if (nextDate.before(originalDate)) {
				previousDate = nextDate;
				continue;
			} else if (nextDate.after(originalDate)) {
				if (previousDate == null || isCloserToNextDate(originalDate, previousDate, nextDate)) {
					return getDays(originalDate, nextDate);
				}
			} else {
				return getDays(originalDate, nextDate);
			}
		}
		return getDays(originalDate, previousDate);
	}
	
	/**
	*	Method to calculate difference in days between two dates
	*  @param   Date originalDate
	*  @param   Date nextDate 
	*  @return  long diffdays (difference in days)
	*/
	private static int getDays(Date date1, Date date2){

		long diff = Math.abs(date1.getTime() - date2.getTime());  //Date difference in milliseconds
		int diffDays = (int)diff / (24 * 60 * 60 * 1000);         //Days in between

		return diffDays;

	}

	

	/**
	 * @param originalDate
	 * @param previousDate
	 * @param nextDate
	 * Method takes the current date and the list of holidays and returns the days 
	 * left to the closest holiday
	 * */
	private static boolean isCloserToNextDate(Date originalDate, Date previousDate, Date nextDate) {
		if(previousDate.after(nextDate))
			throw new IllegalArgumentException("previousDate > nextDate");
		return ((nextDate.getTime() - previousDate.getTime()) / 2 + previousDate.getTime() <= originalDate.getTime());
	}

	/**
	 * @param fldate
	 * Method returns the list of holidays as dates for a year
	 * */
	public static List<Date> getHolidays(Date fldate) throws ParseException{
		List<Date> holidays=new LinkedList<>();
		Calendar cal=Calendar.getInstance();
		cal.setTime(fldate);
		
		// check if New Year's Day
		cal.set(Calendar.MONTH, 11);
		cal.set(Calendar.DATE, 31);
		holidays.add(cal.getTime());
		
		// check if Christmas
		cal.set(Calendar.MONTH, 11);
		cal.set(Calendar.DATE, 25);
		holidays.add(cal.getTime());
		
		// check if Independence Day (4th of July)
		cal.set(Calendar.MONTH, 6);
		cal.set(Calendar.DATE, 4);
		holidays.add(cal.getTime());
		
		// check Thanksgiving (4th Thursday of November)
		cal.set(Calendar.MONTH, 10);
		cal.set(Calendar.DAY_OF_WEEK_IN_MONTH,4);
		cal.set(Calendar.DAY_OF_WEEK, Calendar.THURSDAY);
		holidays.add(cal.getTime());
		
		// check Labor Day (1st Monday of September)
		cal.set(Calendar.MONTH, 8);
		cal.set(Calendar.DAY_OF_WEEK_IN_MONTH,1);
		cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		holidays.add(cal.getTime());
		
		// check President's Day (3rd Monday of February)
		cal.set(Calendar.MONTH, 1);
		cal.set(Calendar.DAY_OF_WEEK_IN_MONTH,3);
		cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		holidays.add(cal.getTime());
		
		// check Veterans Day (November 11)
		cal.set(Calendar.MONTH, 10);
		cal.set(Calendar.DATE, 11);
		holidays.add(cal.getTime());
		
		// check MLK Day (3rd Monday of January)
		cal.set(Calendar.MONTH, 0);
		cal.set(Calendar.DAY_OF_WEEK_IN_MONTH,3);
		cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		holidays.add(cal.getTime());
		
		// check Columbus Day (2nd Monday of October)
		cal.set(Calendar.MONTH, 9);
		cal.set(Calendar.DAY_OF_WEEK_IN_MONTH,2);
		cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		holidays.add(cal.getTime());
		return holidays;	
	}

}
