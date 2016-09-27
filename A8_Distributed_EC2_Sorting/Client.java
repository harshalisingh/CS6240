//@Author: Harshali, Akanksha, Vishal, Saahil


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Client implements Runnable {
	ArrayList<String> finalSortedData = null;
	Thread thread = null;
	ArrayList<IPObject> IPs = null;
	static HashMap<Integer, ArrayList<String>> map = new HashMap<>();
	int sectionRecieved = 0;
	static ArrayList<Socket> clients = null;
	List<String> sortedLocalData = null;
	PrintStream ps = null;
	DataObject obj = new DataObject();
	BufferedReader bf = null;
	static int myIndex = -1;
	int i = 0;
	Socket mySocket = null;
	List<FileData> tempSorted=null;

	public Client(List<String> ip, List<FileData> sortedData) {

		finalSortedData = new ArrayList<String>();
		sortedLocalData = new ArrayList<String>();
		tempSorted=sortedData;
		IPs = readAddressesToadd(ip);

		for (int i = 0; i < sortedData.size(); i++) {
			sortedLocalData.add(sortedData.get(i).toString());
		}

		clients = new ArrayList<Socket>();
		thread = new Thread(this);
		thread.start();
	}
	
	/**
	 * This method reads the list of IPaddress and creates list of IPObjects
	 * */
	public ArrayList<IPObject> readAddressesToadd(List<String> ip) {
		ArrayList<IPObject> obj = new ArrayList<IPObject>();
		for (int i = 0; i < ip.size(); i++) {
			IPObject ob = new IPObject();
			ob.IP = ip.get(i);
			ob.serverPort = 4002;
			obj.add(ob);
		}
		return obj;
	}

	public void run() {
		boolean checkForm=false;
		while (thread != null) {

			String myIP = "";

			try { 
				myIP= FileReader.readMyIP("myip.txt"); }
			catch (IOException e1) {

			 }

			while (i < IPs.size()) {
				try {

					if (IPs.get(i).IP.equals(myIP)) {

						myIndex = i;
						Socket socket = new Socket(IPs.get(i).IP, IPs.get(i).serverPort);
						ps=new PrintStream(socket.getOutputStream());
						bf = new BufferedReader(new InputStreamReader(socket.getInputStream()));

						clients.add(socket);
						mySocket = socket;

					} else {		
						Socket socket = new Socket(IPs.get(i).IP, IPs.get(i).serverPort);
						clients.add(socket);		
					}
					i++;
				} catch (Exception e) {
					
				}
			}

			if(!checkForm){
				sendToClient(clients.get(0), IPs.size()+"");
				checkForm=true;
			}			
			fetchData();			
		}
	}
	
	/**
	 * This method sends the data to its own IP and flush the streams
	 * */
	public void sendForMyIP(Socket socket,String msg){
		try{
			ps = new PrintStream(socket.getOutputStream());
			ps.println("MyIP>"+msg);
			ps.println("done");
			ps.flush();
		}catch(Exception e){
			
		}

	}
	
	/**
	 * This method fetches data from the server and invokes performPhase3 method
	 * */

	public void fetchData() {
		String obj = null;

		try {
			ArrayList<String> list = new ArrayList<String>();
			boolean check = false;


			obj=bf.readLine();

			while ((obj!=null) && !(obj.equalsIgnoreCase("done"))) {

				if (obj.startsWith("splitForm")) {
					obj = obj.substring(obj.indexOf(">") + 1, obj.length());
					list.add(obj);
					check = true;
				} else {
					list.add(obj);
				}
				obj=bf.readLine();
			}

			if (check) {

				performPhase3(list, map);
				sendPhase3Data(map);
				finalSortedData.addAll(map.get(myIndex));
				sectionRecieved++;

			} else {
				finalSortedData.addAll(list);
				sectionRecieved++;
			}
		} catch (Exception e) {

		}
		if (sectionRecieved >= clients.size()) {
			printData();
			thread = null;			
		}
	}
	
	/**
	 *  This method outputs the final sorted data for each node 
	 */
	public void printData() {
		writeOutput(finalSortedData);
		ArrayList<String> list = new ArrayList<String>();
		list.add("close");
		sendToClientFinalSort(clients.get(myIndex), list);
	}

	
	private void writeOutput(ArrayList<String> finalSortedData) {
		try {
			File file = new File("~/output/finaloutput.txt");
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(finalSortedData.toString());
			bw.close();
			System.out.println("Done");

		} catch (IOException e) {

		}

	}
	
	/**
	 * This method gets the individual respective sections from the server 
	 * */

	public void sendPhase3Data(HashMap<Integer, ArrayList<String>> map) {
		int i = 0;
		final HashMap<Integer, ArrayList<String>> tempMap = map;
		for (final Integer key : map.keySet()) {
			final int j = i;
			if (key != i) {
				new Thread(new Runnable() {
					public void run() {

						sendToClientFinalSort(clients.get(j), tempMap.get(key));
					}
				}).start();
			}
			i++;
		}
	}
	
	/**
	 * This method sends the final sort to all the nodes
	 * */
	public void sendToClientFinalSort(Socket socket, ArrayList<String> list) {
		try {
			PrintStream ps = new PrintStream(socket.getOutputStream());
			for (int i = 0; i < list.size(); i++) {
				ps.println(list.get(i));
			}
			ps.println("done");
			ps.flush();

		} catch (Exception e) {
		
		}
	}

	/**
	 * This method computes p-1 pivots from a list of sorted samples
	 * */
	public void performPhase3(ArrayList<String> data, HashMap<Integer, ArrayList<String>> map) {
		int counter = 0;
		int start = 0;
		int size = sortedLocalData.size();
		int i = 0;
		while (counter < data.size() && i < size) {
			double dry = tempSorted.get(i).getDryBulbTemp();

			String temp=data.get(counter).toString().split(",")[3];
			double pivot = Double.parseDouble((temp.substring(temp.indexOf("=")+1, temp.length()-1).trim()));

			if (dry > pivot) {
				ArrayList<String> subList = new ArrayList<String>(sortedLocalData.subList(start, i));
				map.put(counter, subList);
				start = i;
				counter++;
			}
			i++;
		}

		ArrayList<String> subList = new ArrayList<String>(sortedLocalData.subList(start, sortedLocalData.size()));
		map.put(counter, subList);
	}

	/**
	 * This method takes the first row of the sorted samples from each node and append "splitForm" 
	 * and send the data to the server
	 * */
	public void sendToClient(Socket socket, String message) {
		try {
			String firstRow = "splitForm<"+message+">" + sortedLocalData.get(0);
			ps = new PrintStream(socket.getOutputStream());
			ps.println(firstRow);

			for (int i = 1; i < clients.size(); i++) {
				int w = sortedLocalData.size() / clients.size();
				ps.println(sortedLocalData.get(i * w));
			}
			ps.println("done");
			ps.flush();
		} catch (Exception e) {
			
		}
	}
}
