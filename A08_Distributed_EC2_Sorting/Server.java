//@Author: Harshali, Akanksha, Vishal, Saahil


import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class Server implements Runnable{	
	static ServerSocket sk;
	PrintStream ps;
	Thread t;
	static int maxClient=0;
	static boolean checkForm=false;
	static String Myownclient="";
	ClientObject co;
	ArrayList<ClientObject> userList;
	int i = 0, j;

	static ArrayList<String> list = new ArrayList<>();
	public Server() {
		try {
			userList = new ArrayList<ClientObject>();
			sk = new ServerSocket(4002);
			t = new Thread(this);
			t.start();

		} catch (Exception e) {
		}

	}

	@SuppressWarnings("deprecation")
	public void run() {
		try {
			Socket s;
			while (t != null) {
				s = sk.accept();
				adduser(s);
				System.out.println(s);
				Communication cm = new Communication(this, s);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		t.stop();
	}

	public void adduser(Socket sock) {
		ClientObject obj=new ClientObject(sock,sock.getInetAddress());
		userList.add(obj);
	}

	/**
	 * This method sends the data to all the clients present in the userList
	 * */
	public void sendmessage(Socket sk, String msg, InetAddress username,String mes) {
		int m_userListSize = userList.size();
		for (j = 0; j < m_userListSize; j++) {
			co = (ClientObject) userList.get(j);
			{
				sendtoclient(co.getSocket(),msg);
			}		
		}
	}

	
	public void sendtoclient(Socket socket, String message) {
		try {
			
			ps = new PrintStream(socket.getOutputStream());
			ps.println(message);
			ps.flush();;

		} catch (Exception e) {
		}
	}
	public static void main(String arg[]) {
		Server server=new Server();		
	}
}
