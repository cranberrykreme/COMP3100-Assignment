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

public class Worst_fit {

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
	private static final String RESC = "RESC Avail";
	private static final String OK = "OK";
	private static final String ERR2 = "ERR: invalid command (OK)";
	
	public Worst_fit(String address, int port) {
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
			File file = new File("/home/Downloads/ds-sim/system.xml");
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
			int i = 0;
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
				writeMSG(socket,OK);//sends ok
				
				servers = readMSG(socket);//first server info
				
				String foundServer = null;
				
				//writing OK while receiving info on servers,
				//also checks if all info has been sent
				while(!servers.substring(0, 1).contains(".")) {
					
					if(foundServer == null) {
						foundServer = wf(servers, error);
					}

					writeMSG(socket,OK);
					servers = readMSG(socket); //going through the servers available
					
				}

				//job message to server
				if(foundServer == null) {
					writeMSG(socket,"SCHD " + i + " " + ans + " 0");
				} else {
					String servernum = getNumb(foundServer,1);
					foundServer = getNumb(foundServer,0);
					writeMSG(socket,"SCHD " + i + " " + foundServer + " " +servernum);
				}
				
				
				//get response
				String response = readMSG(socket);
				if(response.contains(NONE) || response.contains(ERR)) {
					break;
				}
				
				//send REDY
				writeMSG(socket, REDY);
				i++;
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
	
	
	/**
	 * finds if a server can hold a certain job
	 * if not returns null
	 */
	public static String wf(String server, String error) {
		
		int server_cores = Integer.parseInt(getNumb(server,4));
		int job_cores = Integer.parseInt(getNumb(error,4));
		
		int server_mem = Integer.parseInt(getNumb(server,5));
		int job_mem = Integer.parseInt(getNumb(error,5));
		
		int server_disk= Integer.parseInt(getNumb(server,6));
		int job_disk = Integer.parseInt(getNumb(error,6));
		
		int serverState = Integer.parseInt(getNumb(server,2));
		
		
		int worstFit = Integer.MIN_VALUE;
		int altFit = Integer.MIN_VALUE;
		
		String wf_server = null;
		String af_server = null;
		
		int fitness_val = server_cores - job_cores;
		
			
		/**
		 * Check if sufficient resources are available for a job
		 */
		
		
		if( server_cores >= job_cores && server_mem > job_mem && server_disk > job_disk) {
			
			if(fitness_val > worstFit &&  (serverState == 2 ||serverState == 3))
			{
				
				worstFit = fitness_val;
				wf_server = server;
			}
			
			else if(fitness_val > altFit && (serverState != 2 ||serverState != 3))
			{
				altFit = fitness_val;
				af_server = server;
				
			}
		
		}
		
		if(wf_server != null) {
			return wf_server;
		}
		else if(af_server != null) {
			return af_server;
		}
		else {
			
		//last line	
			
		}
		
		return wf_server;
		
	}
	
	
	/**
	 * Calculate Fitness value
	 */
	
	

	
	/**
	 * Finds the number after a certain space
	 * from both the job and the server information
	 * memory info is held after space 5
	 * diskspace info is held after space 6
	 */
	public static String getNumb(String address, int spaces) {
		int spc = 0;
		int subindex = 0;
		String numb = null;
		
		if(address.length() < 10) {
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
		Worst_fit wf = new Worst_fit("127.0.0.1", 50000);
	}

}
