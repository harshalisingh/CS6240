import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class FlightWritable implements Writable {
	
	private int year;
	private int month;
	private int dayOfMonth;
	private int dayOfWeek;
	private String carrier;
	private String flightDate;
	private int flightNum;
	
	private int originId;
	private int destId;
	
	private int crsDepartureTime;
	private int crsArrivalTime;
	
	private int delay;
	private int daysTillNearestHoliday;
	
	public FlightWritable(){

	}
	
	public FlightWritable(int year, int month, int dayOfMonth, int dayOfWeek, String carrier, int flightNum, String flightDate,
						  int originId, int destId, int  crsDepartureTime, int crsArrivalTime,
			              int delay, int daysTillNearestHoliday) {
		
		this.year = year;
		this.month = month;
		this.dayOfMonth = dayOfMonth;
		this.dayOfWeek = dayOfWeek;
		this.carrier = carrier;
		this.flightNum = flightNum;
		this.flightDate = flightDate;
		this.originId = originId;
		this.destId = destId;
		this.crsDepartureTime = crsDepartureTime;
		this.crsArrivalTime = crsArrivalTime;
		this.delay = delay;
		this.daysTillNearestHoliday = daysTillNearestHoliday;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.year = in.readInt();
		this.month = in.readInt();
		this.dayOfMonth = in.readInt();
		this.dayOfWeek = in.readInt();
		this.carrier = in.readUTF();
		this.flightNum = in.readInt();
		this.flightDate = in.readUTF();
		this.originId = in.readInt();
		this.destId = in.readInt();
		this.crsDepartureTime = in.readInt();
		this.crsArrivalTime = in.readInt();
		this.delay = in.readInt();
		this.daysTillNearestHoliday = in.readInt();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(this.year);
		out.writeInt(this.month);
		out.writeInt(this.dayOfMonth);
		out.writeInt(this.dayOfWeek);
		out.writeUTF(this.carrier);
		out.writeInt(this.flightNum);
		out.writeUTF(this.flightDate);
		out.writeInt(this.originId);
		out.writeInt(this.destId);
		out.writeInt(this.crsDepartureTime);
		out.writeInt(this.crsArrivalTime);
		out.writeInt(this.delay);
		out.writeInt(this.daysTillNearestHoliday);
		
	}
	
	/*
	 * Get and set functions for the key components
	 */
	
	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}
	
	public int getDayOfMonth() {
		return dayOfMonth;
	}

	public void setDayOfMonth(int dayOfMonth) {
		this.dayOfMonth = dayOfMonth;
	}

	public int getDayOfWeek() {
		return dayOfWeek;
	}

	public void setDayOfWeek(int dayOfWeek) {
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
	
	public int getFlightNum() {
		return flightNum;
	}

	public void setFlightNum(int flightNum) {
		this.flightNum = flightNum;
	}
	
	//Origin

	public int getOriginId() {
		return originId;
	}

	public void setOriginId(int originId) {
		this.originId = originId;
	}
	
	public int getDestId() {
		return destId;
	}

	public void setDestId(int destId) {
		this.destId = destId;
	}
	
	public int getCrsArrivalTime() {
		return crsArrivalTime;
	}

	public void setCrsArrivalTime(int crsArrivalTime) {
		this.crsArrivalTime = crsArrivalTime;
	}

	public int getCrsDepartureTime() {
		return crsDepartureTime;
	}

	public void setCrsDepartureTime(int crsDepartureTime) {
		this.crsDepartureTime = crsDepartureTime;
	}
	
	public int getDelay(){
		return delay;
	}
	
	public void setDelay(int delay) {
		this.delay = delay;
	}
	
	public int getDaysTillNearestHoliday(){
		return daysTillNearestHoliday;
	}
	
	public void setDaysTillNearestHoliday(int daysTillNearestHoliday) {
		this.daysTillNearestHoliday = daysTillNearestHoliday;
	}
	
	@Override
	public String toString() {
		
		String str = null;
		
		str = "Flight [year=" + year + ", month=" + month + ", carrier=" + carrier 
				+ ", dayOfMonth=" + dayOfMonth + ", dayOfWeek=" + dayOfWeek
				+ ", flightDate=" + flightDate + ", flightNum=" + flightNum
				+ ", origin=" + originId + ", dest=" + destId 
				+ ", crsArrivalTime=" + crsArrivalTime + ", crsDepartureTime=" + crsDepartureTime
				+ ", arrivalDelay=" + delay + ", daysTillNearestHoliday=" + daysTillNearestHoliday + "]";
		
		return str;
	}


	

	
	
	
	

}
