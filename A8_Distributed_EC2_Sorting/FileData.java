//@Author: Harshali, Akanksha, Vishal, Saahil

import java.io.Serializable;

public class FileData implements Comparable<FileData>, Serializable  {

	private static final long serialVersionUID = 1L;
	private Integer wban;
	private Integer yearMonthDay;
	private Integer time;
	private Double dryBulbTemp;

	public Integer getWban() {
		return wban;
	}
	public void setWban(Integer wban) {
		this.wban = wban;
	}
	public Integer getYearMonthDay() {
		return yearMonthDay;
	}
	public void setYearMonthDay(Integer yearMonthDay) {
		this.yearMonthDay = yearMonthDay;
	}

	public Integer getTime() {
		return time;
	}
	public void setTime(Integer time) {
		this.time = time;
	}

	public Double getDryBulbTemp() {
		return dryBulbTemp;
	}
	public void setDryBulbTemp(Double dryBulbTemp) {
		this.dryBulbTemp = dryBulbTemp;
	}


	/**
	 * Default constructor
	 */
	public FileData() {
		setWban(0);
		setYearMonthDay(0);
		setTime(0);
		setDryBulbTemp(0.0);
	}

	@Override
	public String toString() {
		String str = null;

		str = "FileData: [" + "wban = " + wban + 
				", yearMonthDay = "+ yearMonthDay + 
				", time = " + time +
				", dryBulbTemp = " + dryBulbTemp + "]";

		return str;
	}

	@Override
	public int compareTo(FileData data) {

		Double dryBulbTemp = ((FileData)data).dryBulbTemp;

		if (this.dryBulbTemp > dryBulbTemp)
			return 1;
		else if (this.dryBulbTemp < dryBulbTemp)
			return -1;
		else
			return 0;

	}	

	@Override
	public boolean equals(Object obj) {

		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final FileData other = (FileData) obj;

		if (this.wban != other.wban) {
			return false;
		}
		if (this.yearMonthDay != other.yearMonthDay) {
			return false;
		}
		if (this.time != other.time) {
			return false;
		}
		if (this.dryBulbTemp != other.dryBulbTemp) {
			return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		int hash = 5;
		hash = 47 * hash + this.wban;
		hash = 47 * hash + this.yearMonthDay;
		hash = 47 * hash + this.time;
		hash = (int) (47 * hash + this.dryBulbTemp);
		return hash;
	}

}
