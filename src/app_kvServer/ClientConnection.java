package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import shared.messages.KVM;
import shared.messages.KVMessage.StatusType;

public class ClientConnection implements Runnable {

	private Logger logger = Logger.getLogger("Client Connection");

	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;

	private KVServer kvServer;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * 
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(KVServer kvServer, Socket clientSocket) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.kvServer = kvServer;
	}

	public void close() {
		this.isOpen = false;
	}

	/**
	 * Initializes and starts the client connection.
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();

			// TODO: CHANGE FROM EMPTY SPACE TO SOMETHING ELSE
			sendMessage(new KVM(StatusType.MESSAGE, " ",
					"Connection to KV server established "
							+ clientSocket.getLocalAddress() + " "
							+ clientSocket.getLocalPort()));

			// TODO: More informative logs on server side.
			while (isOpen) {
				try {
					KVM latestMsg = receiveMessage();
					StatusType status = latestMsg.getStatus();
					logger.info(status);

					if (status.equals(StatusType.PUT)) {
						String key = latestMsg.getKey();
						String value = latestMsg.getValue();
						status = StatusType.PUT_ERROR;

						try {
							this.kvServer.putKV(key, value);
							logger.info(key + value);
							status = StatusType.PUT_SUCCESS;
						} catch (Exception e) {
							// Log message
						}

						sendMessage(new KVM(status, key, value));

					} else if (status.equals(StatusType.GET)) {
						String key = latestMsg.getKey();
						status = StatusType.GET_ERROR;
						String value = " "; // TODO Change from empty string

						try {
							value = this.kvServer.getKV(latestMsg.getKey());
							status = StatusType.GET_SUCCESS;
							logger.info(value);
						} catch (Exception e) {
							// Log Message
						}

						sendMessage(new KVM(status, key, value));

					} else if (status.equals(StatusType.DELETE)) {
						String key = latestMsg.getKey();
						String value = latestMsg.getValue();
						status = StatusType.DELETE_ERROR;

						try {
							this.kvServer.deleteKV(key);
							status = StatusType.DELETE_SUCCESS;
							logger.info(key + value);
						} catch (Exception e) {
							// Log message
						}

						sendMessage(new KVM(status, key, value));

						// } else if (msgParts[0].equals("kill")) {
						// this.kvServer.kill();
						// sendMessage(new TextMessage("success"));
						// } else if (msgParts[0].equals("close")) {
						// this.kvServer.close();
						// sendMessage(new TextMessage("success"));
					}
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!", ioe);
					isOpen = false;
				} catch (Exception e) {
					logger.error("Error! Connection lost!", e);
					isOpen = false;
				}
			}

		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!");

		} finally {

			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!");
			}
		}
	}

	/**
	 * Method sends a message using this socket.
	 * 
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream
	 */
	public void sendMessage(KVM msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND \t<"
				+ clientSocket.getInetAddress().getHostAddress() + ":"
				+ clientSocket.getPort() + ">: '"
				+ msg.getMsg() + "'");
	}

	private KVM receiveMessage() throws IOException {

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();
		boolean reading = true;

		while (/* read != 13 && */ read != 10 && read != -1 && reading) {/* CR, LF, error */
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

			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;

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
		logger.info("RECEIVE \t<"
				+ clientSocket.getInetAddress().getHostAddress() + ":"
				+ clientSocket.getPort() + ">: '"
				+ msg.getMsg().trim() + "'");
		return msg;
	}

}
