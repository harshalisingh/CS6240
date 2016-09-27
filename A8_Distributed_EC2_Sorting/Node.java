//@Author: Harshali, Akanksha, Vishal, Saahil


import java.io.IOException;
import java.util.List;




public class Node {

	public static String INPUT;
	public static String OUTPUT;

	public static void main(String[] args) throws IOException, InterruptedException{

		INPUT = "input";

		List<String> ipList = FileReader.getAllIP("ipList.txt");
		List<FileData> fileData = FileReader.readFile(INPUT);

		int numNodes = ipList.size();

		// Sample Sort Algorithm
		SampleSort ss = new SampleSort(numNodes);
		
		// Gets local sorted data
		List<FileData> sortedLocalData =ss.sortLocalData(fileData); 

		Server ws = new Server();
		Client wc = new Client(ipList,sortedLocalData);

	}

}
