
/**
 * @author Harshali Singh
 *
 */

import java.io.IOException;

public class FlightParser {
	
	
	/**
	 * Method to parse each row and return a Flight object for history data
	 * 
	 */
	public static Flight getFlightData(String[] row) throws IOException {
		Flight flight = new Flight();

		flight.setYear(Integer.parseInt(row[FlightConstants.INDEX_FLIGHT_YEAR].trim()));
		flight.setMonth(Integer.parseInt(row[FlightConstants.INDEX_FLIGHT_MONTH].trim()));
		flight.setDayOfMonth(Integer.parseInt(row[FlightConstants.INDEX_FLIGHT_DAY_OF_MONTH].trim()));
		flight.setDayOfWeek(Integer.parseInt(row[FlightConstants.INDEX_FLIGHT_DAY_OF_WEEK].trim()));
		
		flight.setCarrier(row[FlightConstants.INDEX_FLIGHT_CARRIER].trim());
		flight.setFlightDate(row[FlightConstants.INDEX_FLIGHT_DATE].trim());
		flight.setFlightNum(Integer.parseInt(row[FlightConstants.INDEX_FLIGHT_NUM].trim()));
		
		flight.setOrigin(row[FlightConstants.INDEX_ORIGIN].trim());
		flight.setOriginAirportId(Integer.parseInt(row[FlightConstants.INDEX_ORIGIN_AIRPORT_ID].trim()));
		flight.setOriginAirportSequenceId(Integer.parseInt(row[FlightConstants.INDEX_ORIGIN_AIRPORT_SEQUENCE_ID].trim()));
		flight.setOriginCityMarketId(Integer.parseInt(row[FlightConstants.INDEX_ORIGIN_CITY_MARKET_ID].trim()));		
		flight.setOriginStateFips(Integer.parseInt(row[FlightConstants.INDEX_ORIGIN_STATE_FIPS].trim()));
		flight.setOriginWac(Integer.parseInt(row[FlightConstants.INDEX_ORIGIN_WAC].trim()));
		flight.setOriginCityName(row[FlightConstants.INDEX_ORIGIN_CITY_NAME].trim());
		flight.setOriginStateAbbr(row[FlightConstants.INDEX_ORIGIN_STATE_ABBR].trim());
		flight.setOriginStateName(row[FlightConstants.INDEX_ORIGIN_STATE_NAME].trim());
		
		flight.setDest(row[FlightConstants.INDEX_DEST].trim());
		flight.setDestAirportId(Integer.parseInt(row[FlightConstants.INDEX_DEST_AIRPORT_ID].trim()));
		flight.setDestAirportSequenceId(Integer.parseInt(row[FlightConstants.INDEX_DEST_AIRPORT_SEQUENCE_ID].trim()));
		flight.setDestCityMarketId(Integer.parseInt(row[FlightConstants.INDEX_DEST_CITY_MARKET_ID].trim()));		
		flight.setDestStateFips(Integer.parseInt(row[FlightConstants.INDEX_DEST_STATE_FIPS].trim()));
		flight.setDestWac(Integer.parseInt(row[FlightConstants.INDEX_DEST_WAC].trim()));
		flight.setDestCityName(row[FlightConstants.INDEX_DEST_CITY_NAME].trim());
		flight.setDestStateAbbr(row[FlightConstants.INDEX_DEST_STATE_ABBR].trim());
		flight.setDestStateName(row[FlightConstants.INDEX_DEST_STATE_NAME].trim());
		
		if (!row[FlightConstants.INDEX_ACT_ARR_TIME].isEmpty() && !row[FlightConstants.INDEX_ACT_DEP_TIME].isEmpty()
				&& !row[FlightConstants.INDEX_ACT_ELAPSED_TIME].isEmpty()){
			
			flight.setCrsArrivalTime(Integer.parseInt(row[FlightConstants.INDEX_CRS_ARR_TIME].trim()));
			flight.setCrsDepartureTime(Integer.parseInt(row[FlightConstants.INDEX_CRS_DEP_TIME].trim()));
			flight.setCrsElapsedTime(Integer.parseInt(row[FlightConstants.INDEX_CRS_ELAPSED_TIME].trim()));
			flight.setActualArrivalTime(Integer.parseInt(row[FlightConstants.INDEX_ACT_ARR_TIME].trim()));		
			flight.setActualDepartureTime(Integer.parseInt(row[FlightConstants.INDEX_ACT_DEP_TIME].trim()));
			flight.setActualElapsedTime(Integer.parseInt(row[FlightConstants.INDEX_ACT_ELAPSED_TIME].trim()));
			
		}
		
		if (!row[FlightConstants.INDEX_ARR_DELAY].isEmpty()){

			flight.setArrivalDelay(Double.parseDouble(row[FlightConstants.INDEX_ARR_DELAY].trim()));
			flight.setArrivalDelayMinutes(Double.parseDouble(row[FlightConstants.INDEX_ARR_DELAY_MINS].trim()));
			flight.setArrivalDelay15(Double.parseDouble(row[FlightConstants.INDEX_ARR_DELAY_15].trim()));
		}

		flight.setCancelled(Integer.parseInt(row[FlightConstants.INDEX_CANCELLED].trim()));
		flight.setPrice(Double.parseDouble(row[FlightConstants.INDEX_PRICE].trim()));

		return flight;
	}
	
	
	/**
	 * Method to parse each row and return a Flight object for test data
	 * 
	 */
	public static Flight getFlightData(String[] row, boolean isTest) throws IOException {
		
		Flight flight = new Flight();


		flight.setYear(Integer.parseInt(row[FlightConstants.INDEX_FLIGHT_YEAR].trim()));
		flight.setMonth(Integer.parseInt(row[FlightConstants.INDEX_FLIGHT_MONTH].trim()));
		flight.setDayOfMonth(Integer.parseInt(row[FlightConstants.INDEX_FLIGHT_DAY_OF_MONTH].trim()));
		flight.setDayOfWeek(Integer.parseInt(row[FlightConstants.INDEX_FLIGHT_DAY_OF_WEEK].trim()));
		
		flight.setCarrier(row[FlightConstants.INDEX_FLIGHT_CARRIER].trim());
		flight.setFlightNum(Integer.parseInt(row[FlightConstants.INDEX_FLIGHT_NUM].trim()));
		flight.setFlightDate(row[FlightConstants.INDEX_FLIGHT_DATE].trim());
		
		flight.setOriginAirportId(Integer.parseInt(row[FlightConstants.INDEX_ORIGIN_AIRPORT_ID].trim()));
		flight.setDestAirportId(Integer.parseInt(row[FlightConstants.INDEX_DEST_AIRPORT_ID].trim()));
		
		flight.setOrigin(row[FlightConstants.INDEX_ORIGIN].trim());
		flight.setDest(row[FlightConstants.INDEX_DEST].trim());
		
		flight.setCrsArrivalTime(Integer.parseInt(row[FlightConstants.INDEX_CRS_ARR_TIME].trim()));
		flight.setCrsDepartureTime(Integer.parseInt(row[FlightConstants.INDEX_CRS_DEP_TIME].trim()));
		flight.setCrsElapsedTime(Integer.parseInt(row[FlightConstants.INDEX_CRS_ELAPSED_TIME].trim()));
		
		return flight;
	}

}
