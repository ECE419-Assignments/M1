package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.io.IOException;

import logger.LogSetup;

// import java.util.logging.Logger;
// import java.util.logging.Level;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import shared.messages.KVMessage.StatusType;

public class KVServer extends Thread implements IKVServer {

	private static Logger logger = Logger.getLogger("KV Server");
	private HashMap<String, String> kvMap = new HashMap<String, String>();
	/**
	 * Start KV Server at given port
	 * 
	 * @param port      given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *                  to keep in-memory
	 * @param strategy  specifies the cache replacement strategy in case the cache
	 *                  is full and there is a GET- or PUT-request on a key that is
	 *                  currently not contained in the cache. Options are "FIFO",
	 *                  "LRU",
	 *                  and "LFU".
	 */

	// private static Logger logger = Logger.getRootLogger();

	private int port;
	private CacheStrategy strategy;
	private boolean running;
	private int cacheSize;
	private ServerSocket serverSocket;
	private Cache cache;

	public KVServer(int port, int cacheSize, CacheStrategy strategy) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;
		this.running = false;
		this.cache = new Cache(cacheSize);
	}

	public int getPort() {
		return port;
	}

	//TODO: Make this not hardcoded
	public String getHostname() {
		return "127.0.0.1";
	}

	public CacheStrategy getCacheStrategy() {
		return strategy;
	}

	public int getCacheSize() {
		return cacheSize;
	}

	public boolean inStorage(String key) {
		return cache.onDisk(key);
	}

	public boolean inCache(String key) {
		return cache.containsKey(key);
	}

	public String getKV(String key) throws Exception {
		return cache.find(key);
	}

	public void putKV(String key, String value) throws Exception {
		logger.info("putting value");
		cache.save(key, value);
	}

	public void clearCache() {
		cache.clearCache();
	}

	public void clearStorage() {
		cache.clearDisk();
	}

	public void run() {
		running = initializeServer();

		if (serverSocket != null) {
			while (isRunning()) {
				try {
					logger.info("opening connection");
					Socket client = serverSocket.accept();
					ClientConnection connection = new ClientConnection(this, client);
					new Thread(connection).start();

					logger.info("Connected to "
							+ client.getInetAddress().getHostName()
							+ " on port " + client.getPort());
				} catch (IOException e) {
					logger.info("Error! " +
							"Unable to establish connection. \n");
				}
			}
			logger.info("done with while");
		}
	}

	public void kill() {
		logger.info("Killing server!");
		System.exit(0);
	}

	public void close() {
		logger.info("Closing server!");
		this.running = false;
		System.exit(0);
	}

	private boolean isRunning() {
		return this.running;
	}

	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: "
					+ serverSocket.getLocalPort());
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if (e instanceof BindException) {
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}

	public static void main(String[] args) {
		try {
			new LogSetup("logs/server.log", Level.ALL);
			if (args.length != 1) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
			} else {
				int port = Integer.parseInt(args[0]);
				KVServer kvServer = new KVServer(port, 10, CacheStrategy.FIFO);
				kvServer.start();
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
	}
}
