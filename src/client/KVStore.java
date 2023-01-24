package client;

import java.net.Socket;

import shared.messages.KVMessage;

public class KVStore implements KVCommInterface {
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */

	private String address;
	private int port;
	private Client client = null; //TODO: We don't know what this class is

	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
	}

	@Override
	public void connect() throws Exception {
		throws UnknownHostException, IOException {
		client = new Client(address, port);
		
		new Socket(address, port);
		// listeners = new HashSet<ClientSocketListener>();
		logger.info("Connection established");

		// client.addListener(this);
		// client.start();
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
}
