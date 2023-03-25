package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.io.IOException;

import logger.LogSetup;
import shared.ecs.ECSNode;
import shared.metadata.KVMetadata;
import shared.misc;
import shared.messages.KVM;
import shared.messages.KVMessage.StatusType;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.exceptions.FailedException;
import app_kvServer.exceptions.KeyNotFoundException;
import app_kvServer.exceptions.ServerNotResponsibleException;
import app_kvServer.exceptions.ServerStoppedException;
import app_kvServer.exceptions.WriteLockException;

public class KVServer extends Thread implements IKVServer {
	public enum KVServerResponseCode {
		SERVER_NOT_RESPONSIBLE("::SERVER_NOT_RESPONSIBLE");

		private final String text;

		/**
		 * @param text
		 */
		KVServerResponseCode(final String text) {
			this.text = text;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Enum#toString()
		 */
		@Override
		public String toString() {
			return text;
		}
	};

	private static Logger logger = Logger.getLogger("KV Server");
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
	private LinkedHashMap<String, Cache> replicas_caches;
	protected String[] hash_range;
	private int ecsPort;
	public volatile KVMetadata metadata;
	private ECSConnection connectionECS;

	protected boolean serverStopped = true;

	public KVServer(int port, int cacheSize, CacheStrategy strategy, int ecsPort) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;
		this.ecsPort = ecsPort;
		this.cache = new Cache(cacheSize, "localhost", port);
		this.serverStopped = true;
		this.metadata = new KVMetadata();
		this.start();

	}

	@Override
	public String[] getNodeHashRange() throws ServerStoppedException {
		if (this.serverStopped) {
			throw new ServerStoppedException();
		}
		return hash_range;
	}

	public int getPort() {
		return port;
	}

	public String getHostname() {
		return "localhost";
	}

	public String getAddress() {
		return String.format("%s:%s", this.getHostname(), this.getPort());
	}

	public CacheStrategy getCacheStrategy() throws ServerStoppedException {
		if (this.serverStopped) {
			throw new ServerStoppedException();
		}
		return strategy;
	}

	public int getCacheSize() throws ServerStoppedException {
		if (this.serverStopped) {
			throw new ServerStoppedException();
		}
		return cacheSize;
	}

	public boolean inStorage(String key) throws ServerStoppedException {
		if (this.serverStopped) {
			throw new ServerStoppedException();
		}
		if (this.serverStopped) {
			throw new ServerStoppedException();
		}

		return cache.onDisk(key);
	}

	public boolean inCache(String key) throws ServerStoppedException {
		if (this.serverStopped) {
			throw new ServerStoppedException();
		}
		return cache.containsKey(key);
	}

	public String getKV(String key)
			throws ServerNotResponsibleException, FailedException, KeyNotFoundException, ServerStoppedException,
			ServerNotResponsibleException {
		if (this.serverStopped) {
			throw new ServerStoppedException();
		}

		String responsibleServerAddress = this.metadata.getKeysServer(key).getNodeAddress();
		String currentServerAddress = this.getAddress();

		if (responsibleServerAddress.equals(currentServerAddress)) {
			return cache.find(key);
		} else if (this.metadata.isServerReplicaOf(currentServerAddress, responsibleServerAddress)) {
			if (replicas_caches.containsKey(key)) {
				return replicas_caches.get(responsibleServerAddress).find(key);
			} else {
				throw new FailedException();
			}
		} else {
			throw new ServerNotResponsibleException();
		}

	}

	public void deleteKV(String key, boolean forceDelete)
			throws ServerStoppedException, WriteLockException, KeyNotFoundException,
			ServerNotResponsibleException {
		if (this.serverStopped) {
			throw new ServerStoppedException();
		}

		logger.debug(String.format("Deleting key value for key", key));

		ECSNode keys_server = this.metadata.getKeysServer(key);
		String address = this.getAddress();

		if ((keys_server.getNodeAddress()).equals(address) || forceDelete) {
			logger.debug(String.format("Trying to delete", key));
			cache.delete(key, forceDelete);
		} else {
			throw new ServerNotResponsibleException();
		}

	}

	public void putKV(String key, String value)
			throws ServerNotResponsibleException, ServerStoppedException, WriteLockException,
			ServerNotResponsibleException {
		if (this.serverStopped) {
			throw new ServerStoppedException();
		}
		ECSNode keys_server = this.metadata.getKeysServer(key);
		String address = this.getAddress();

		if ((keys_server.getNodeAddress()).equals(address)) {
			cache.save(key, value);
		} else {
			throw new ServerNotResponsibleException();
		}
	}

	public void putReplicaKV(String key, String value)
			throws ServerNotResponsibleException, ServerStoppedException, WriteLockException,
			ServerNotResponsibleException, FailedException {
		if (this.serverStopped) {
			throw new ServerStoppedException();
		}
		String responsible_server_address = this.metadata.getKeysServer(key).getNodeAddress();

		if (replicas_caches.containsKey(responsible_server_address)) {
			replicas_caches.get(responsible_server_address).save(key, value);
		} else {
			throw new FailedException();
		}
	}

	public void deleteReplicaKV(String key, boolean forceDelete)
			throws ServerStoppedException, WriteLockException, KeyNotFoundException,
			ServerNotResponsibleException, FailedException {
		if (this.serverStopped) {
			throw new ServerStoppedException();
		}

		logger.debug(String.format("Deleting replica key value for key", key));

		String responsible_server_address = this.metadata.getKeysServer(key).getNodeAddress();

		if (replicas_caches.containsKey(responsible_server_address)) {
			logger.debug(String.format("Trying to delete", key));
			replicas_caches.get(responsible_server_address).delete(key, forceDelete);
		} else {
			throw new FailedException();
		}

	}

	public void clearCache() throws ServerStoppedException, WriteLockException {
		if (this.serverStopped) {
			throw new ServerStoppedException();
		}
		cache.clearCache();
	}

	public void clearStorage() throws ServerStoppedException, WriteLockException {
		if (this.serverStopped) {
			throw new ServerStoppedException();
		}
		cache.clearDisk();
	}

	public LinkedHashMap<String, String> getAllKeyValues() {
		return this.cache.getAllKeyValues();
	}

	public void run() {
		running = initializeServer();

		Runtime.getRuntime().addShutdownHook(new KVServerHook(this.connectionECS));

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

	public void stopServer() {
		this.serverStopped = true;
	}

	public boolean isServerStopped() {
		return this.serverStopped;
	}

	public void startServer() {
		this.serverStopped = false;
	}

	public void close() {
		logger.info("Closing server!");
		this.running = false;
		System.exit(0);
	}

	private boolean isRunning() {
		return this.running;
	}

	protected void setWriteLock(boolean locked) {
		cache.setWriteLock(locked);
	}

	protected boolean getWriteLock() {
		return cache.getWriteLock();
	}

	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);

			// Start ECS Socket
			this.connectionECS = new ECSConnection(this, "localhost", ecsPort);
			new Thread(this.connectionECS).start();

			logger.info("Server listening on port: "
					+ serverSocket.getLocalPort());
			return true;

		} catch (IOException | InterruptedException e) {
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
			if (args.length != 2) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port> <ecs port>!");
			} else {
				int port = Integer.parseInt(args[0]);
				int ecsPort = Integer.parseInt(args[1]);
				KVServer kvServer = new KVServer(port, 10, CacheStrategy.FIFO, ecsPort);
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

	///////////////////// Milestone /////////////////////

	// private KVHasher hasher;

	// public boolean getAllData() { // TODO: Zeni
	// return false;
	// }

	// public boolean getDataFromHashrange(String hashrange) { // TODO: Zeni
	// return false;
	// }

	// public void shutdown() {

	// }

	// public void deleteAllData() { // TODO: Zeni

	// }

	// public void sendAllDataToServer(ECSNode node) {
	// this.serverConnection.connect(node).sendData(this.getAllData()).disconnect();
	// }

	// public void sendDataToServer(ECSNode node, String hashrange) {
	// this.serverConnection.connect(node).sendData(this.getDataFromHashrange(hashrange)).disconnect();
	// }

	// public void updateMetadata(String key_range) {
	// this.hasher.updateServerTree(key_range);
	// }
}
