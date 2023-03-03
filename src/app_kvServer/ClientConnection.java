package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.exceptions.FailedException;
import app_kvServer.exceptions.KeyNotFoundException;
import app_kvServer.exceptions.ServerNotResponsibleException;
import app_kvServer.exceptions.ServerStoppedException;
import app_kvServer.exceptions.WriteLockException;
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
	private KVM latestMsg;

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
					String key = latestMsg.getKey();
					String value = latestMsg.getValue();
					logger.info(status);

					StatusType responseStatus = StatusType.FAILED;
					String responseKey = key;
					String responseValue = value;

					try {
						if (status.equals(StatusType.PUT)) {
							responseStatus = StatusType.PUT_ERROR;

							boolean alreadyExists = this.kvServer.inCache(key);

							this.kvServer.putKV(key, value);
							logger.info(key + value);

							if (!alreadyExists) {
								responseStatus = StatusType.PUT_SUCCESS;
							} else {
								responseStatus = StatusType.PUT_UPDATE;
							}
						} else if (status.equals(StatusType.GET)) {
							value = this.kvServer.getKV(latestMsg.getKey());
							status = StatusType.GET_SUCCESS;
							logger.info(value);
						} else if (status.equals(StatusType.DELETE)) {
							this.kvServer.deleteKV(key);
							responseStatus = StatusType.DELETE_SUCCESS;
							logger.info(key + value);
						} else if (status.equals(StatusType.KEYRANGE)) {
							responseValue = String.join(";", this.kvServer.getNodeHashRange());
							// TODO: M2 - Turn into a string that we can pass back to client
						}
					} catch (ServerStoppedException e) {
						responseStatus = StatusType.SERVER_STOPPED;
					} catch (ServerNotResponsibleException e) {
						responseStatus = StatusType.SERVER_NOT_RESPONSIBLE;
					} catch (WriteLockException e) {
						responseStatus = StatusType.SERVER_WRITE_LOCK;
					} catch (KeyNotFoundException e) {
						responseStatus = StatusType.FAILED;
						if (status.equals(StatusType.DELETE)) {
							responseStatus = StatusType.DELETE_ERROR;
						} else if (status.equals(StatusType.GET)) {
							responseStatus = StatusType.GET_ERROR;
						}
					} catch (Exception e) {
						responseStatus = StatusType.FAILED;
						responseValue = e.getMessage();
					}

					sendMessage(new KVM(responseStatus, responseKey, responseValue));
				} catch (IOException ioe) {
					System.out.println(ioe);
					logger.error("Error! Connection lost!", ioe);
					isOpen = false;
				} catch (NumberFormatException e) {
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

	public KVM getLatestMsg() {
		return latestMsg;
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

	private KVM receiveMessage() throws IOException, Exception {

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
