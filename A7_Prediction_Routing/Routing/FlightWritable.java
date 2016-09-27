

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class FlightWritable implements WritableComparable<FlightWritable> {
	
	public int year;
	public int month;
	public int dayOfMonth;
	public int dayOfWeek;
	public String carrier;
	public String flightDate;
	public int flightNum;
	
	public int originId;
	public int destId;
	
	public String origin;
	public String dest;
	
	public int crsDepartureTime;
	public int crsArrivalTime;
	public int crsElapsedTime;
	
	//public int actDepartureTime;
	//public int actArrivalTime;
	
	
	public long scheduledTime;
	
	//public long actualTime;
	
	//public int cancelled;

	public int delay;
	public int daysTillNearestHoliday;
	
	public Boolean isArrival;	

	public FlightWritable(){

	}
	
	public FlightWritable(int year, int month, int dayOfMonth, int dayOfWeek, String carrier, int flightNum, String flightDate,
						  int originId, int destId, String origin, String dest, int  crsDepartureTime, int crsArrivalTime,
						  int crsElapsedTime, long scheduledTime,
			              int delay, int daysTillNearestHoliday, Boolean isArrival) {
		
		this.year = year;
		this.month = month;
		this.dayOfMonth = dayOfMonth;
		this.dayOfWeek = dayOfWeek;
		this.carrier = carrier;
		this.flightNum = flightNum;
		this.flightDate = flightDate;
		this.originId = originId;
		this.destId = destId;
		this.origin = origin;
		this.dest = dest;
		
		this.crsDepartureTime = crsDepartureTime;
		this.crsArrivalTime = crsArrivalTime;
		this.crsElapsedTime = crsElapsedTime;
		
		//this.actDepartureTime = actDepartureTime;
		//this.actArrivalTime = actArrivalTime;
	
		
		this.scheduledTime = scheduledTime;
		//this.actualTime = actualTime;
		//this.cancelled = cancelled;
		this.delay = delay;
		this.daysTillNearestHoliday = daysTillNearestHoliday;
		this.isArrival = isArrival;
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
		
		this.origin = in.readUTF();
		this.dest = in.readUTF();
		
		this.crsDepartureTime = in.readInt();
		this.crsArrivalTime = in.readInt();
		this.crsElapsedTime = in.readInt();
		
		//this.actDepartureTime = in.readInt();
		//this.actArrivalTime = in.readInt();
		
		
		this.scheduledTime = in.readLong();
		//this.actualTime = in.readLong();
		
		//this.cancelled = in.readInt();
		
		this.delay = in.readInt();
		this.daysTillNearestHoliday = in.readInt();
		this.isArrival = in.readBoolean();
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
		
		out.writeUTF(this.origin);
		out.writeUTF(this.dest);
		
		out.writeInt(this.crsDepartureTime);
		out.writeInt(this.crsArrivalTime);
		out.writeInt(this.crsElapsedTime);
		
		//out.writeInt(this.actDepartureTime);
		//out.writeInt(this.actArrivalTime);
		
		
		out.writeLong(this.scheduledTime);
		
		//out.writeLong(this.actualTime);
		
		//out.writeInt(this.cancelled);
		
		out.writeInt(this.delay);
		out.writeInt(this.daysTillNearestHoliday);
		out.writeBoolean(this.isArrival);
		
	}
	
	/*
	 * Get and set functions for the key components
	 */
	
	/*public int getYear() {
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
	
	public String getOrigin() {
		return origin;
	}

	public void setOrigin(String origin) {
		this.origin = origin;
	}

	public String getDest() {
		return dest;
	}

	public void setDest(String dest) {
		this.dest = dest;
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
	
	public int getActDepartureTime() {
		return actDepartureTime;
	}

	public void setActDepartureTime(int actDepartureTime) {
		this.actDepartureTime = actDepartureTime;
	}

	public int getActArrivalTime() {
		return actArrivalTime;
	}

	public void setActArrivalTime(int actArrivalTime) {
		this.actArrivalTime = actArrivalTime;
	}
	
	public long getScheduledTime() {
		return scheduledTime;
	}

	public void setScheduledTime(long scheduledTime) {
		this.scheduledTime = scheduledTime;
	}

	public long getActualTime() {
		return actualTime;
	}

	public void setActualTime(long actualTime) {
		this.actualTime = actualTime;
	}
	
	public int getCancelled() {
		return cancelled;
	}

	public void setCancelled(int cancelled) {
		this.cancelled = cancelled;
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
	
	public Boolean getIsArrival() {
		return isArrival;
	}

	public void setIsArrival(Boolean isArrival) {
		this.isArrival = isArrival;
	}*/
	
	
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
	
	@Override
	public int compareTo(FlightWritable fo) {
		
		// compare the 2 objects based on scheduledTime
		long flightScheduledTime = fo.scheduledTime;

		long diff = this.scheduledTime - flightScheduledTime;

		if (diff < 0)
			return -1;
		if (diff == 0)
			return 0;
		return 1;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		//result = prime * result + (int) (actualTime ^ (actualTime >>> 32));
		//result = prime * result + cancelled;
		result = prime * result + (int) (scheduledTime ^ (scheduledTime >>> 32));
		result = prime * result + ((isArrival == null) ? 0 : isArrival.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FlightWritable other = (FlightWritable) obj;
		if (scheduledTime != other.scheduledTime)
			return false;
		if (isArrival == null) {
			if (other.isArrival != null)
				return false;
		} else if (!isArrival.equals(other.isArrival))
			return false;
		return true;
	}

	
	
	
	

}
