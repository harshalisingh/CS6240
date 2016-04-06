//@Author: Harshali, Akanksha, Vishal, Saahil

import java.net.InetAddress;
import java.net.Socket;
public class ClientObject
{
	ClientObject(Socket socket,InetAddress UserName)
	{
		ClientSocket = socket;
		ClientUserName = UserName;		
	}

	public void setSocket(Socket socket)
	{
		ClientSocket =  socket;
	}

	public void setIP(InetAddress UserName)
	{
		ClientUserName = UserName;
	}

	public Socket getSocket()
	{
		return ClientSocket;
	}

	public InetAddress getIP()
	{
		return ClientUserName;
	}

	public Socket ClientSocket;
	public InetAddress ClientUserName;
}