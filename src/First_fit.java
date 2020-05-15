import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * 
 * @author chrispurkiss
 * 
 * First fit is an algorithm to send
 * a job to the first available or capable server
 * with the capacity to run that job
 */

public class First_fit {

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
	private static final String RESCCapable = "RESC Capable";
	private static final String OK = "OK";
	
	public First_fit(String address, int port) {
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
				
				writeMSG(socket,OK);//sends ok
				servers = readMSG(socket);//first server info
				String foundServer = null;
				//writing OK while receiving info on servers,
				//also checks if all info has been sent
				while(!servers.substring(0, 1).contains(".")) {
					String isAvail = getNumb(servers, 3);
					
					double isa = Double.parseDouble(isAvail);
					
					if(foundServer == null && isa >=0) {
						foundServer = ff(servers, error);
					}

					writeMSG(socket,OK);
					servers = readMSG(socket); //going through the servers available
					
				}
				String jobN = getNumb(error, 2);
				 if(foundServer != null) {
					String servernum = getNumb(foundServer,1);
					foundServer = getNumb(foundServer,0);
					writeMSG(socket,"SCHD " + jobN + " " + foundServer + " " +servernum);
				}
				 else {
					 writeMSG(socket, RESCCapable + job);
						
						String serversCap = readMSG(socket);//sends back DATA
						
						writeMSG(socket,OK);//sends ok
						serversCap = readMSG(socket);//first server info
						
						while(!serversCap.substring(0, 1).contains(".")) {
							
							if(foundServer == null) {
								if(Integer.parseInt(getNumb(serversCap,2)) == 3 || Integer.parseInt(getNumb(serversCap,2)) ==2 ) {
								foundServer = ff(serversCap, error);
								}
							}

							writeMSG(socket,OK);
							serversCap = readMSG(socket); //going through the servers available
							
						}
						
						String servernum = getNumb(foundServer,1);
						foundServer = getNumb(foundServer,0);
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
	/**
	 * finds if a server can hold a certain job
	 * if not returns null
	 */
	public static String ff(String address, String job) {
		String hold = null;
		
		String memory = null;
		String diskspace = null;
		
		String jobMem = null;
		String jobDisk = null;

		memory = getNumb(address,5);//gets memory for server
		diskspace = getNumb(address,6);//gets diskspace for server
		
		jobMem = getNumb(job,5);//gets memory for job
		jobDisk = getNumb(job,6);//gets diskspace for job
		
		/**
		 * if the memory and diskspace of the server is large enough
		 * to hold the job then set the server to that server
		 * if not, then leave it as null
		 */
		if(Double.parseDouble(memory) > Double.parseDouble(jobMem) && Double.parseDouble(diskspace) > Double.parseDouble(jobDisk)) {
			hold = address;
		}
		
		return hold;
		
	}
	
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
		
		if(address.length() < 10) {//just in case a shorter message (such as DATA gets through)
			return null;
		}
		
		for(int temp = 0; temp < address.length(); temp++) {//gets the index of the required space
			if(address.charAt(temp) == ' ') {
				spc++;
			}
			if(spc == spaces) {
				subindex = temp;
				break;
			}
		}
		
		int finalIndex = subindex +1;
		if(spaces <= 5) {//if the space is not for the final number in the message
			while(address.charAt(finalIndex) != ' ') {
				finalIndex++;
			}
		} else {//if it is
			finalIndex = address.length();
		}
		
		if(spaces != 0) {//if the space is for the name of the server/job
			numb = address.substring(subindex+1,finalIndex);
		} else {//if it is not
			numb = address.substring(subindex,finalIndex);
		}
		
		return numb;
	}
	
	/*
	 * initialize connection
	 */
	public static void main(String[] args) {
		First_fit ff = new First_fit("127.0.0.1", 50000);
	}

}
