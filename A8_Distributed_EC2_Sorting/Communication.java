//@Author: Harshali, Akanksha, Vishal, Saahil


import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.io.PrintStream;
import java.net.Socket;
import java.util.Collections;

public class Communication implements Runnable {
	Server sr;
	Socket skt;
	BufferedReader bf;
	PrintStream ps;
	Thread t;
	int i = 0;

	public Communication(Server srvr, Socket s) {
		sr = srvr;
		skt = s;
		try {
			ps = new PrintStream(skt.getOutputStream());
			bf = new BufferedReader(new InputStreamReader(skt.getInputStream()));

			t = new Thread(this);
			t.start();

		} catch (Exception e) {
			
		}
	}

	public void run() {
		try {

			while (t != null) {

				String obj = bf.readLine();

				boolean checkForm = false;
				while ((obj != null) && !(obj.equalsIgnoreCase("done"))) {
					if(obj.startsWith("MyIP>")){
						obj=obj.substring(obj.indexOf(">")+1,obj.length());
						Server.Myownclient=obj;
					}
					if (obj.startsWith("splitForm<")) {

						Server.maxClient=Integer.parseInt(obj.charAt(10)+"");
						obj = obj.substring(obj.indexOf(">") + 1, obj.length());
						checkForm = true;
					}else if(obj.equalsIgnoreCase("close")){
						t = null;
						sr.t = null;
					}
					if (checkForm) {
						Server.list.add(obj);
					}
					else if(!checkForm){

						sr.sendmessage(skt, obj, skt.getInetAddress(),"onlyme");
					}

					obj=bf.readLine();
				}

				if(Server.list.size()>=Math.pow(Server.maxClient, 2) &&checkForm){
					
					Collections.sort(Server.list);
					performPhase2Work();
					checkForm=false;

				}
			}

		} catch (Exception e) {
			
		}		
	}

	/**
	 * This method calculates the pivots from the sample list
	 * */
	public void performPhase2Work() {
		int rho=sr.userList.size()/2;		
		int p=sr.userList.size();
		for(int i=0;i<sr.userList.size()-1;i++){			
			String data=Server.list.get(((i+1)*p)+rho-1);
			sr.sendmessage(skt,"splitForm>"+data, skt.getInetAddress(),"all");
		}
		System.out.println();
		for(int j=0;j<sr.userList.size();j++){
			sr.sendtoclient(sr.userList.get(j).getSocket(), "done");
		}

	}
}
