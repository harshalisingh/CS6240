
/**
 * @author Harshali Singh
 *
 */
public class Flight {
	
	private Integer year;
	private Integer month;
	private Integer dayOfMonth;
	private Integer dayOfWeek;
	private String carrier;
	private String flightDate;
	private Integer flightNum;
	
	private String origin;
	private Integer originAirportId;
	private Integer originAirportSequenceId;
	private Integer originCityMarketId;
	private Integer originStateFips;
	private Integer originWac;
	private String originCityName;
	private String originStateAbbr;
	private String originStateName;
	
	private String dest;
	private Integer destAirportId;
	private Integer destAirportSequenceId;
	private Integer destCityMarketId;
	private Integer destStateFips;
	private Integer destWac;
	private String destCityName;
	private String destStateAbbr;
	private String destStateName;
	
	private int crsArrivalTime;
	private int crsDepartureTime;
	private int crsElapsedTime;
	private int actualArrivalTime;
	private int actualDepartureTime;
	private int actualElapsedTime;
	private double departureDelay;
	private double arrivalDelay;
	private double arrivalDelayMinutes;
	private double arrivalDelay15;
	private int cancelled;
	private double price;
	
	/*
	 * Getters & Setters
	 */

	public Integer getYear() {
		return year;
	}

	public void setYear(Integer year) {
		this.year = year;
	}

	public Integer getMonth() {
		return month;
	}

	public void setMonth(Integer month) {
		this.month = month;
	}
	
	public Integer getDayOfMonth() {
		return dayOfMonth;
	}

	public void setDayOfMonth(Integer dayOfMonth) {
		this.dayOfMonth = dayOfMonth;
	}

	public Integer getDayOfWeek() {
		return dayOfWeek;
	}

	public void setDayOfWeek(Integer dayOfWeek) {
		this.dayOfWeek = dayOfWeek;
	}


	public String getCarrier() {
		return carrier;
	}

	public void setCarrier(String carrier) {
		this.carrier = carrier;
	}
	
	public String getFlightDate() {
		return flightDate;
	}

	public void setFlightDate(String flDate) {
		this.flightDate = flDate;
	}
	
	public Integer getFlightNum() {
		return flightNum;
	}

	public void setFlightNum(Integer flightNum) {
		this.flightNum = flightNum;
	}
	
	//Origin

	public String getOrigin() {
		return origin;
	}

	public void setOrigin(String origin) {
		this.origin = origin;
	}

	public Integer getOriginAirportId() {
		return originAirportId;
	}

	public void setOriginAirportId(Integer originAirportId) {
		this.originAirportId = originAirportId;
	}

	public Integer getOriginAirportSequenceId() {
		return originAirportSequenceId;
	}

	public void setOriginAirportSequenceId(Integer originAirportSequenceId) {
		this.originAirportSequenceId = originAirportSequenceId;
	}

	public Integer getOriginCityMarketId() {
		return originCityMarketId;
	}

	public void setOriginCityMarketId(Integer originCityMarketId) {
		this.originCityMarketId = originCityMarketId;
	}

	public Integer getOriginStateFips() {
		return originStateFips;
	}

	public void setOriginStateFips(Integer originStateFips) {
		this.originStateFips = originStateFips;
	}

	public Integer getOriginWac() {
		return originWac;
	}

	public void setOriginWac(Integer originWac) {
		this.originWac = originWac;
	}

	public String getOriginCityName() {
		return originCityName;
	}

	public void setOriginCityName(String originCityName) {
		this.originCityName = originCityName;
	}

	public String getOriginStateAbbr() {
		return originStateAbbr;
	}

	public void setOriginStateAbbr(String originStateAbbr) {
		this.originStateAbbr = originStateAbbr;
	}

	public String getOriginStateName() {
		return originStateName;
	}

	public void setOriginStateName(String originStateName) {
		this.originStateName = originStateName;
	}
	
	//dest
	
	public String getDest() {
		return dest;
	}

	public void setDest(String dest) {
		this.dest = dest;
	}

	public Integer getDestAirportId() {
		return destAirportId;
	}

	public void setDestAirportId(Integer destAirportId) {
		this.destAirportId = destAirportId;
	}

	public Integer getDestAirportSequenceId() {
		return destAirportSequenceId;
	}

	public void setDestAirportSequenceId(Integer destAirportSequenceId) {
		this.destAirportSequenceId = destAirportSequenceId;
	}

	public Integer getDestCityMarketId() {
		return destCityMarketId;
	}

	public void setDestCityMarketId(Integer destCityMarketId) {
		this.destCityMarketId = destCityMarketId;
	}

	public Integer getDestStateFips() {
		return destStateFips;
	}

	public void setDestStateFips(Integer destStateFips) {
		this.destStateFips = destStateFips;
	}

	public Integer getDestWac() {
		return destWac;
	}

	public void setDestWac(Integer destWac) {
		this.destWac = destWac;
	}

	public String getDestCityName() {
		return destCityName;
	}

	public void setDestCityName(String destName) {
		this.destCityName = destName;
	}

	public String getDestStateAbbr() {
		return destStateAbbr;
	}

	public void setDestStateAbbr(String destStateAbbr) {
		this.destStateAbbr = destStateAbbr;
	}

	public String getDestStateName() {
		return destStateName;
	}

	public void setDestStateName(String destStateName) {
		this.destStateName = destStateName;
	}

	public int getCrsArrivalTime() {
		return crsArrivalTime;
	}

	public void setCrsArrivalTime(Integer crsArrivalTime) {
		this.crsArrivalTime = crsArrivalTime;
	}

	public int getCrsDepartureTime() {
		return crsDepartureTime;
	}

	public void setCrsDepartureTime(Integer crsDepartureTime) {
		this.crsDepartureTime = crsDepartureTime;
	}

	public int getCrsElapsedTime() {
		return crsElapsedTime;
	}

	public void setCrsElapsedTime(Integer crsElapsedTime) {
		this.crsElapsedTime = crsElapsedTime;
	}

	public int getActualArrivalTime() {
		return actualArrivalTime;
	}

	public void setActualArrivalTime(Integer actualArrivalTime) {
		this.actualArrivalTime = actualArrivalTime;
	}

	public int getActualDepartureTime() {
		return actualDepartureTime;
	}

	public void setActualDepartureTime(Integer actualDepartureTime) {
		this.actualDepartureTime = actualDepartureTime;
	}

	public int getActualElapsedTime() {
		return actualElapsedTime;
	}

	public void setActualElapsedTime(Integer actualElapsedTime) {
		this.actualElapsedTime = actualElapsedTime;
	}

	public double getDepartureDelay() {
		return departureDelay;
	}

	public void setDepartureDelay(double departureDelay) {
		this.departureDelay = departureDelay;
	}

	public double getArrivalDelay() {
		return arrivalDelay;
	}

	public void setArrivalDelay(double arrivalDelay) {
		this.arrivalDelay = arrivalDelay;
	}

	public double getArrivalDelayMinutes() {
		return arrivalDelayMinutes;
	}

	public void setArrivalDelayMinutes(double arrivalDelayMinutes) {
		this.arrivalDelayMinutes = arrivalDelayMinutes;
	}

	public double getArrivalDelay15() {
		return arrivalDelay15;
	}

	public void setArrivalDelay15(double arrivalDelay15) {
		this.arrivalDelay15 = arrivalDelay15;
	}

	public Integer getCancelled() {
		return cancelled;
	}

	public void setCancelled(Integer cancelled) {
		this.cancelled = cancelled;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}
	

	/**
	 * Default constructor
	 */
	public Flight() {
		
		setYear(0);
		setMonth(0);
		setDayOfMonth(0);
		setDayOfWeek(0);
		setCarrier(new String());
		setFlightDate(new String());
		setFlightNum(0);
		
		setOrigin(new String());
		setOriginAirportId(0);
		setOriginAirportSequenceId(0);
		setOriginCityMarketId(0);
		setOriginStateFips(0);
		setOriginWac(0);
		setOriginCityName(new String());
		setOriginStateAbbr(new String());
		setOriginStateName(new String());
		
		setDest(new String());
		setDestAirportId(0);
		setDestAirportSequenceId(0);
		setDestCityMarketId(0);
		setDestStateFips(0);
		setDestWac(0);
		setDestCityName(new String());
		setDestStateAbbr(new String());
		setDestStateName(new String());
		
		setCrsArrivalTime(0);
		setCrsDepartureTime(0);
		setCrsElapsedTime(0);
		setActualArrivalTime(0);
		setActualDepartureTime(0);
		setActualElapsedTime(0);
		setArrivalDelay(0);
		setArrivalDelayMinutes(0);
		setArrivalDelay15(0);
		setCancelled(0);
		setPrice(0);
		
	}
	
	@Override
	public String toString() {
		
		String str = null;
		
		str = "Flight [year=" + year + ", month=" + month + ", carrier=" + carrier 
				+ ", dayOfMonth=" + dayOfMonth + ", dayOfWeek=" + dayOfWeek
				+ ", flightDate=" + flightDate + ", flightNum=" + flightNum
				+ ", origin=" + origin + ", originAirportId=" + originAirportId  + ", originAirportSequenceId=" + originAirportSequenceId 
				+ ", originCityMarketId=" + originCityMarketId + ", originStateFips=" + originStateFips + ", originWac=" + originWac 
				+ ", originCityName=" + originCityName + ", originStateAbbr=" + originStateAbbr + ", originStateName=" + originStateName
				+ ", dest=" + dest + ", destAirportId=" + destAirportId + ", destAirportSequenceId=" + destAirportSequenceId 
				+ ", destCityMarketId=" + destCityMarketId + ", destStateFips=" + destStateFips + ", destWac=" + destWac
				+ ", destCityName=" + destCityName + ", destStateAbbr=" + destStateAbbr + ", destStateName=" + destStateName 
				+ ", crsArrivalTime=" + crsArrivalTime + ", crsDepartureTime=" + crsDepartureTime + ", crsElapsedTime=" + crsElapsedTime
				+ ", actualArrivalTime=" + actualArrivalTime + ", actualDepartureTime=" + actualDepartureTime
				+ ", actualElapsedTime=" + actualElapsedTime + ", arrivalDelay=" + arrivalDelay
				+ ", arrivalDelayMinutes=" + arrivalDelayMinutes + ", arrivalDelay15=" + arrivalDelay15 + ", cancelled=" + cancelled 
				+ ", price=" + price + "]";
		
		return str;
	}

}
