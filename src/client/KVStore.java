package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

public class KVStore extends Thread implements KVCommInterface {
	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address the address of the KVServer
	 * @param port    the port of the KVServer
	 */

	private boolean running;
	private Logger logger = Logger.getLogger("KVStore");
	private Set<KVClient> listeners;
	private Socket clientSocket;
	private String address;
	private int port;
	private static volatile KVM latestMsg;
	private static volatile int msgCount = 0;

	private OutputStream output;
	private InputStream input;

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

	public void run() {
		logger.info("starting the KVStore");
		try {
			this.output = this.clientSocket.getOutputStream();
			this.input = this.clientSocket.getInputStream();

			while (isRunning()) {
				try {
					latestMsg = receiveMessage();
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

	@Override
	public void connect() throws UnknownHostException, IOException {
		this.clientSocket = new Socket(address, port);
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

		/* read first char from stream */
		byte read = (byte) input.read();
		boolean reading = true;

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
		System.out.println(getLatestMsg().getStatus());
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
		return getNextMsg();
	}

	@Override
	public KVM getKeyrange() throws IOException {
		sendMessage(new KVM(StatusType.KEYRANGE, "", ""));
		return getNextMsg();
	}

	// TODO: CHANGE FROM EMPTY SPACE TO SOMETHING ELSE
	@Override
	public KVM get(String key) throws IOException {
		sendMessage(new KVM(StatusType.GET, key, " "));
		return getNextMsg();
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
