//@Author: Harshali, Vishal, Akanksha, Saahil



import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class FileReader {

	/**
	 * This method reads the input file and creates a FileData object
	 * */
	public static FileData getFileData(String[] row){

		FileData record = new FileData();

		try{

			record.setWban(Integer.parseInt(row[FileConstants.INDEX_WBAN_NUMBER].trim()));
			record.setYearMonthDay(Integer.parseInt(row[FileConstants.INDEX_YEARMONTHDAY].trim()));
			record.setTime(Integer.parseInt(row[FileConstants.INDEX_TIME].trim()));
			record.setDryBulbTemp(Double.parseDouble(row[FileConstants.INDEX_DRY_BULB_TEMP].trim()));
		} catch (NumberFormatException ex) {
		
		}

		return record;

	}

	/**
	 * This method gets all the IP addresses of all the running instances
	 **/	

	public static List<String> getAllIP(String ipFile) throws IOException {

		// Open the file
		FileInputStream fstream = new FileInputStream(ipFile);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

		List<String> ipList = new ArrayList<String>();
		String strLine;

		//Read File Line By Line
		while ((strLine = br.readLine()) != null)   {
			if(!strLine.trim().equals("null")){
				ipList.add(strLine.trim());
			}		  
		}

		//Close the input stream
		br.close();

		return ipList;

	}

	/**
	 * This method gets the IP address of its own instance
	 * */

	public static String readMyIP(String fileName) throws IOException {
		FileInputStream fstream = new FileInputStream(fileName);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
		try {
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();

			while (line != null) {
				sb.append(line);
				sb.append("\n");
				line = br.readLine();
			}
			return sb.toString();
		} finally {
			br.close();
		}
	}

	/**
	 * This method reads all the input files
	 * */

	public static List<FileData> readFile(String folder) throws IOException {

		// Open the file
		File dir = new File(folder);
		File[] files = dir.listFiles();
		String strLine;

		List<FileData> fileData = new ArrayList<FileData>();
		InputStream fileStream;
		InputStream gzipStream;
		Reader reader;
		BufferedReader br = null;

		for(File f : files) {
			if(f.getName().endsWith("gz")){
				fileStream = new FileInputStream(f);
				gzipStream = new GZIPInputStream(fileStream);
				reader = new InputStreamReader(gzipStream, "UTF-8");
				br = new BufferedReader(reader);

				//Read File Line By Line
				while ((strLine = br.readLine()) != null)   {

					String[] row = strLine.split(",");
					FileData temp = getFileData(row);
					fileData.add(temp);

				}
			}
		}

		//Close the input stream
		br.close();
		return fileData;
	}
}
