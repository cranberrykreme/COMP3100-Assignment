import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class Best_fit {
	private Socket socket;
	private OutputStream outToServer;
	private DataOutputStream out;
	private InputStream inFromServer;
	private DataInputStream in;
	
	private static final String HELO =  "HELO";
	private static final String AUTH =  "AUTH comp335";
	private static final String QUIT = "QUIT";
	private static final String REDY = "REDY";
	private static final String NONE = "NONE";
	private static final String ERR = "ERR: No such waiting job exists";
	private static final String RESC = "RESC Capable";
	private static final String OK = "OK";
	private static final String ERR2 = "ERR: invalid command (OK)";
	
	public Best_fit(String address, int port) {
		try {
			System.out.println("Attempting connection with " + address + " at port " + port);
			socket = new Socket(address,port);
			System.out.println("Connected");
			
			//new message to Server
			writeMSG(socket, HELO);
			
			//receive message from Server
			readMSG(socket);
			
			//second message to server
			writeMSG(socket, AUTH);
			
			//parse system.xml
			//File file = new File("/Users/garyguan/Downloads/ds-sim/system.xml");
			//File file = new File("/Users/chrispurkiss/ds-sim/system.xml");
			File file = new File("/home/comp335/ds-sim/system.xml");
			//File file = new File("system.xml");
			String ans = parse(file);
			System.out.println(ans);
			
			//second message from server
			readMSG(socket);
			
			//third message to server
			writeMSG(socket,REDY);
			
			//third message from server
			//readMSG(socket);
			
			/**
			 * method for going through the servers
			 * and picking the right server for each job
			 */
			while(true) {
				//reading job from server
				String error = readMSG(socket);
				if(error.contains(NONE) || error.contains(ERR)) {
					break;
				}
				
				//finding correct resc command for specific job
				int spaces = 0;
				int index = 0;
				for(int temp = 0; temp < error.length(); temp++) {
					if(error.charAt(temp) == ' ') {
						spaces++;
					}
					if(spaces == 4) {
						index = temp;
						break;
					}
				}
				
				//sending RESC command
				String job = error.substring(index);
				writeMSG(socket, RESC + job);
			
				String servers = readMSG(socket);//sends back DATA
				
				
				writeMSG(socket,OK);//sends OK
				
				
				servers = readMSG(socket);//first server info
				String foundServer = null;//to put the final server info into
				
				double bestFit = Double.MAX_VALUE;
				double minAvail = Double.MAX_VALUE;
				
				//writing OK while receiving info on servers,
				//also checks if all info has been sent
				String servertemp =null;
				
				while(!servers.substring(0, 1).contains(".")) {
					double fit =0;
					double availtime =0;
					fit = fitnessvalue(servers, error, 4);
					availtime = fitnessvalue(servers, error, 3);
					
					if(bestFit > fit && fit >= 0) {
						bestFit = fit;
						servertemp = servers;
						minAvail = availtime;
					}
					else if (bestFit == fit && minAvail > availtime) {
						minAvail = availtime;
						servertemp = servers;
					}
				
					writeMSG(socket,OK);
					servers = readMSG(socket); //going through the servers available
					
				}
				
				String jobN = getNumb(error, 2);
				//if no best_fit server is found, return largest server
				if(servertemp == null) {
					writeMSG(socket,"SCHD" + jobN + " "+ans+ " 0");;
				} else {
					String servernum = getNumb(servertemp,1);
					foundServer = getNumb(servertemp,0);
					writeMSG(socket,"SCHD " + jobN + " " + foundServer + " " +servernum);
				}
				
				//get response
				String response = readMSG(socket);
				if(response.contains(NONE) || response.contains(ERR)) {
					break;
				}
				
				//send REDY
				writeMSG(socket, REDY);
			}
			
			//LAST STAGE: QUIT
			writeMSG(socket, QUIT);
			readMSG(socket);
			
		}  catch (UnknownHostException u) {
			System.out.println(u);
		}
		catch (IOException e) {
			System.out.println(e);
		}
		//close all the client communications and the socket
		try {
			inFromServer.close();
			outToServer.close();
			in.close();
			out.close();
			socket.close();
		}  catch (IOException i) {
			System.out.println(i);
		}
	}//end of main class
	
	/*
	 * Get the strings to send
	 * find location to send 
	 * get them in the right format to send
	 * and send them to client
	 * 
	 */
	private void writeMSG(Socket socket, String msg) throws IOException {
		outToServer = socket.getOutputStream();
		out = new DataOutputStream(outToServer);
		
		out.write(msg.getBytes());
		System.out.println("messge sent to server: " + msg);
		out.flush();
	}
	/*
	 * get message from server
	 * print out that message has been received
	 * use message in Client method
	 */
	private String readMSG(Socket socket) throws IOException {
		inFromServer = socket.getInputStream();
		in = new DataInputStream(inFromServer);
		
		byte[] rMSG = new byte[1024];
		in.read(rMSG);
		
		String str = new String(rMSG);
		System.out.println("message received from server: "  + str);
		return str;
	}
	
	/*
	 * get information out of file and return
	 * the largest server (the one with the most cores)
	 * used in this as a backup way of allocating jobs
	 */
	private String parse(File file) {
		try {
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(file);
			ArrayList<String> str = new ArrayList<String>();
			ArrayList<Integer> list = new ArrayList<Integer>();
			
			doc.getDocumentElement().normalize();
			
			NodeList nList = doc.getElementsByTagName("server");
			
			if(doc.hasChildNodes()) {
				for(int i = 0; i < nList.getLength(); i++) {
					Node node = nList.item(i);
					
					if(node.getNodeType() == Node.ELEMENT_NODE) {
						Element element = (Element) node;
						
						str.add(element.getAttribute("type"));
						list.add(Integer.parseInt(element.getAttribute("coreCount")));
					}
				}
			}
			
			int largest = 0;
			
			for(int i = 1; i<str.size(); i++) {
				if(list.get(i) > list.get(largest)) {
					largest  = i;
				}
			}
			
			String ans = new String();
			ans =  str.get(largest);
			
			return ans;
			
		} catch (Exception e) {
			System.out.println(e);
		}
		
		return "Did not work";
	}
	

	
	public static Integer fitnessvalue(String address, String job, int spaces) {
		String servercore = getNumb(address, spaces);
		String jobcore = getNumb(job,spaces);
		
		int fv =0;
		fv = Integer.parseInt(servercore) - Integer.parseInt(jobcore);
		
		return fv;
	}
	
	
	
	public static String isolatecore(String address, int space) {
		int count =0;
		int firstspace = 0;
		int lastspace = 0;
		String corenum = null;
		
		for(int a=0; a<address.length(); a++) {
			if(address.charAt(a)== ' ') {
				count++;
			}
			if(count == space) {
				firstspace = a+1;
			}
			if(count == space+1) {
				lastspace=a;
			}
		}
		
		corenum = address.substring(firstspace, lastspace);
		
		return corenum;
	}
	
	/**
	 * Finds the number after a certain space
	 * from both the job and the server information
	 * available time is held after space 3
	 * #CPU cores is held after space 4
	 * memory info is held after space 5
	 * diskspace info is held after space 6
	 */
	public static String getNumb(String address, int spaces) {
		int spc = 0;
		int subindex = 0;
		String numb = null;
		
		if(address.length() < 5) {
			System.out.println("address is too short at: " + address.length());
			return null;
		}
		
		for(int temp = 0; temp < address.length(); temp++) {
			if(address.charAt(temp) == ' ') {
				spc++;
			}
			if(spc == spaces) {
				subindex = temp;
				break;
			}
		}
		System.out.println(spc + " subindex is: " + subindex);
		System.out.println(address);
		
		
		int finalIndex = subindex +1;
		if(spaces <= 5) {
			while(address.charAt(finalIndex) != ' ') {
				finalIndex++;
			}
		} else {
			finalIndex = address.length();
		}
		
		
		System.out.println("finalindex is: " + finalIndex);
		if(spaces != 0) {
			numb = address.substring(subindex+1,finalIndex);
		} else {
			numb = address.substring(subindex,finalIndex);
		}
		
		
		System.out.println("string returned " + numb);
		
		return numb;
	}
	
	/*
	 * initialize connection
	 */
	public static void main(String[] args) {
		Best_fit bf = new Best_fit("127.0.0.1", 50000);
	}
}
