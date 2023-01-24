package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KVServer implements IKVServer {

	private static Logger logger = Logger.getRootLogger();
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */

	 
	// private static Logger logger = Logger.getRootLogger();

	private int port;
	private String strategy;
	private boolean running;
	private int cacheSize;
	private ServerSocket serverSocket;


	public KVServer(int port, int cacheSize, String strategy) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;
		this.running = false;
	}
	
	public int getPort(){
		return port;
	}

    public String getHostname(){
		// TODO Auto-generated method stub
		return null;
	}

    public CacheStrategy getCacheStrategy(){
		//TODO: Fix so it returns the actual strategy
		return CacheStrategy.None;
		// return strategy;
	}

    public int getCacheSize(){
		return cacheSize;
	}

    public boolean inStorage(String key){
		// TODO Auto-generated method stub
		return false;
	}

    public boolean inCache(String key){
		// TODO Auto-generated method stub
		return false;
	}

    public String getKV(String key) throws Exception{
		// TODO Auto-generated method stub
		return "";
	}

    public void putKV(String key, String value) throws Exception{
		// TODO Auto-generated method stub
	}

    public void clearCache(){
		// TODO Auto-generated method stub
	}

    public void clearStorage(){
		// TODO Auto-generated method stub
	}

    public void run(){
		running = initializeServer();

		if(serverSocket != null) {
	        while(isRunning()){
	            try {
	                Socket client = serverSocket.accept();                
	                // ClientConnection connection = 
	                // 		new ClientConnection(client);
	                // new Thread(connection).start();
	                
	                logger.info("Connected to " 
	                		+ client.getInetAddress().getHostName() 
	                		+  " on port " + client.getPort());
	            } catch (IOException e) {
	            	logger.error("Error! " +
	            			"Unable to establish connection. \n", e);
	            }
	        }
        }
        // logger.info("Server stopped.");
	}

    public void kill(){
		// TODO Auto-generated method stub
	}

    public void close(){
		// TODO Auto-generated method stub
	}

	    
    private boolean isRunning() {
        return this.running;
    }

	private boolean initializeServer() {
    	// logger.info("Initialize server ...");
    	try {
            serverSocket = new ServerSocket(port);
            // logger.info("Server listening on port: " 
            // 		+ serverSocket.getLocalPort());    
            return true;
        
        } catch (IOException e) {
        	// logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
            	// logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
	}
	
	public static void main(String[] args) {
    	try {
			new LogSetup("logs/server.log", Level.ALL);
			if(args.length != 1) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
			} else {
				int port = Integer.parseInt(args[0]);
				// new KVServer(port, 10, CacheStrategy.None).start();
				System.out.print("Setting up ksserver\n");
				new KVServer(port, 10, "None").run();
				System.out.print("Done setting up ksserver\n");
			}
		} 
		catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} 
		catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
    }
}
