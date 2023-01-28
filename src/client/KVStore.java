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
		// TODO Auto-generated method stub
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub. Needs to utilize KVMessage to encode the data
		// then send the request through the socket. (Probably need buffers for this).
		// Need a connected check before executing.
		return null;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub. Needs to utilize KVMessage to encode the data.
		// Then send the request through the socket.
		// Need a connected check before executing.
		return null;
	}
}
