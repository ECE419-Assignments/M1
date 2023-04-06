package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ObjectInputFilter.Status;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import shared.messages.KVM;
import shared.messages.KVMessage.StatusType;
import app_kvClient.KVClient;
import app_kvClient.KVClient.SocketStatus;
import app_kvServer.exceptions.ServerNotResponsibleException;
import shared.metadata.KVMetadata;
import shared.ecs.ECSNode;

public class KVStore extends Thread implements KVCommInterface {
	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address the address of the KVServer
	 * @param port    the port of the KVServer
	 */

	private volatile boolean running;
	private Logger logger = Logger.getLogger("KVStore");
	private Set<KVClient> listeners;
	private Socket clientSocket;
	private String address;
	private int port;
	private static volatile KVM latestMsg;
	private static volatile int msgCount = 0;

	private OutputStream output;
	private InputStream input;

	private volatile boolean stop = false;
	private volatile boolean wait = true;

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

	public KVMetadata metadata = new KVMetadata();

	public void run() {
		logger.info("starting the KVStore");
		try {
			this.output = this.clientSocket.getOutputStream();
			this.input = this.clientSocket.getInputStream();
			sendMessage(new KVM(StatusType.GET_KEYRANGE, "", ""));
			while (isRunning()) {
				while (this.stop) {
					this.wait = false;
				}
				try {
					KVM message = receiveMessage();
					if (message == null) {
						continue;
					}
					latestMsg = message;
					if (latestMsg.getStatus().equals(StatusType.GET_KEYRANGE_SUCCESS)) {
						metadata.createServerTree(latestMsg.getValue());
					}
					msgCount++;
					for (KVClient listener : listeners) {
						listener.handleNewMessage(latestMsg);
					}
				} catch (IOException ioe) {
					if (isRunning()) {
						logger.info("Connection lost!");
						try {
							tearDownConnection();
							for (KVClient listener : listeners) {
								listener.handleStatus(
										SocketStatus.CONNECTION_LOST);
							}
						} catch (IOException e) {
							logger.error("Unable to close connection!");
						}
					}
				} catch (NumberFormatException ex) {
					logger.info("empty string sent");
				} catch (Exception ex) {
					logger.error(ex);
					logger.error("exception");
				}
			}
		} catch (IOException ioe) {
			logger.error("Connection could not be established!");
			disconnect();
		}
	}

	public KVM getLatestMsg() {
		return latestMsg;
	}

	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
	}

	public void newConnection(String address, int port) {
		try {
			this.stop = true;
			this.wait = true;
			while (this.wait) {
			}
			logger.info("tearing down the connection ...");
			input.close();
			output.close();
			this.clientSocket.close();
			this.clientSocket = new Socket(address, port);
			this.output = this.clientSocket.getOutputStream();
			this.input = this.clientSocket.getInputStream();
			this.stop = false;
			logger.info("Connection established");
			try {
				Thread.sleep(100);
			} catch (Exception e) {
			}
		} catch (UnknownHostException e) {
			logger.error("Something went wrong connecting to new server.");
		} catch (IOException e) {
			logger.error("RUH ROH RAGGY");
		}

	}

	@Override
	public void connect() throws UnknownHostException, IOException {
		this.clientSocket = new Socket(this.address, this.port);
		this.listeners = new HashSet<KVClient>();
		setRunning(true);
		logger.info("Connection established");
		this.start();
		try {
			Thread.sleep(100);
		} catch (Exception e) {
		}
	}

	@Override
	public synchronized void disconnect() {
		logger.info("try to close connection ...");

		try {
			tearDownConnection();
			for (KVClient listener : listeners) {
				logger.info("Closing listener");
				listener.handleStatus(SocketStatus.DISCONNECTED);
			}
		} catch (Exception ioe) {
			logger.error("Unable to close connection!");
		}
	}

	public boolean isRunning() {
		return this.running;
	}

	public void setRunning(boolean run) {
		this.running = run;
	}

	public void addListener(KVClient listener) {
		listeners.add(listener);
	}

	private KVM receiveMessage() throws IOException, Exception {

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		byte read;
		boolean reading;

		/* read first char from stream */
		if (input.available() != 0) {
			read = (byte) input.read();
			reading = true;
		} else {
			return null;
		}

		while (read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
			if (index == BUFFER_SIZE) {
				if (msgBytes == null) {
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			}

			/* only read valid characters, i.e. letters and numbers */
			if ((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}

			/* stop reading is DROP_SIZE is reached */
			if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}

			/* read next char from stream */
			read = (byte) input.read();
		}

		if (msgBytes == null) {
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}

		msgBytes = tmp;

		/* build final String */
		KVM msg = new KVM(msgBytes);
		logger.info("Receive message:\t '" + msg.getMsg() + "'");
		return msg;
	}

	public KVM getNextMsg() {
		int prevMsgCount = msgCount;
		while (prevMsgCount == msgCount) {
			prevMsgCount = msgCount;
			try {
				Thread.sleep(1);
			} catch (Exception e) {

			}
		}
		return getLatestMsg();
	}

	public void sendMessage(KVM msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("Send message:\t '" + msg.getMsg() + "'");
	}

	@Override
	public KVM put(String key, String value) throws IOException {
		StatusType status = StatusType.PUT;
		if (value.equals("null")) {
			status = StatusType.DELETE;
		}
		sendMessage(new KVM(status, key, value));
		logger.info("msg");
		KVM message = getNextMsg();
		status = message.getStatus();
		logger.info("after_msg");

		if (status == StatusType.SERVER_NOT_RESPONSIBLE) {
			this.getKeyrange();
			ECSNode server_node = this.metadata.getKeysServer(key);
			String address = server_node.getNodeHost();
			int port = server_node.getNodePort();
			this.newConnection(address, port);
			this.put(key, value);
		}

		return message;
	}

	@Override
	public KVM getKeyrange() throws IOException {
		sendMessage(new KVM(StatusType.GET_KEYRANGE, "", ""));
		KVM message = getNextMsg();
		logger.info("Got key range");
		logger.info(message.getValue());
		metadata.createServerTree(message.getValue());
		return message;
	}

	@Override
	public KVM get(String key) throws IOException {
		sendMessage(new KVM(StatusType.GET, key, " "));
		KVM message = getNextMsg();
		StatusType status = message.getStatus();

		if (status == StatusType.SERVER_NOT_RESPONSIBLE) {
			this.getKeyrange();
			ECSNode server_node = this.metadata.getKeysServer(key);
			String address = server_node.getNodeHost();
			int port = server_node.getNodePort();
			this.newConnection(address, port);
			this.get(key);
		} else if (status == StatusType.SERVER_STOPPED) {
			logger.info("Response: Server is stopped");
		}

		return message;
	}

	private void tearDownConnection() throws IOException {
		setRunning(false);
		logger.info("tearing down the connection ...");
		if (clientSocket != null) {
			input.close();
			output.close();
			clientSocket.close();
			clientSocket = null;
			logger.info("connection closed!");
		}
	}

}

/*
 * Receives messages from Server and passes them to KVClient
 * Used by KVClient to run actions on the server.
 */
