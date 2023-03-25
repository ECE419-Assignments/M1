package app_kvServer;

import java.io.IOException;
import java.net.Socket;

import app_kvServer.exceptions.KeyNotFoundException;
import app_kvServer.exceptions.ServerNotResponsibleException;
import app_kvServer.exceptions.ServerStoppedException;
import app_kvServer.exceptions.WriteLockException;
import shared.BaseConnection;
import shared.messages.KVM;
import shared.messages.KVMessage.StatusType;
import shared.ecs.ECSNode;
import shared.misc;

public class ClientConnection extends BaseConnection {

	private KVServer kvServer;

	public ClientConnection(KVServer kvServer, Socket socket) {
		super(socket);
		this.kvServer = kvServer;
	}

	@Override()
	public void processMessage(KVM message) throws IOException {
		StatusType status = message.getStatus();
		String key = message.getKey();
		String value = message.getValue();
		logger.info(status);

		StatusType responseStatus = StatusType.FAILED;
		String responseKey = key;
		String responseValue = value;
		boolean sendResponse = false;

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
				sendResponse = true;

				//Update replicas
				ECSNode[] replicas = this.kvServer.metadata.getReplicaNodes(this.kvServer.getAddress());
				logger.info("updating replicas");
				for(ECSNode node : replicas){
					if (node != null){
						String server_address = node.getNodeAddress();
						String host = misc.getHostFromAddress(server_address);
						int port = misc.getPortFromAddress(server_address);
						Socket socket = new Socket(host, port);

						ClientConnection connection = new ClientConnection(this.kvServer, socket);
						new Thread(connection).start();
						Thread.sleep(500);

						connection.sendMessage(new KVM(StatusType.PUT_REPLICA, key, value));

						Thread.sleep(100);
						connection.close();
					}
				}
			} else if (status.equals(StatusType.GET)) {
				responseValue = this.kvServer.getKV(message.getKey());
				responseStatus = StatusType.GET_SUCCESS;
				sendResponse = true;
			} else if (status.equals(StatusType.DELETE)) {
				this.kvServer.deleteKV(key, false);
				responseStatus = StatusType.DELETE_SUCCESS;
				logger.info(key + value);
				sendResponse = true;
			} else if (status.equals(StatusType.GET_KEYRANGE)) {
				responseValue = this.kvServer.metadata.getKeyRange();
				responseStatus = StatusType.GET_KEYRANGE_SUCCESS;
				sendResponse = true;
			} else if (status.equals(StatusType.PUT_REPLICA)){
				// TODO: Navid - replica put this.kvServer.putKVReplica(key, value)
				responseStatus = StatusType.PUT_REPLICA_SUCCESS;
				sendResponse = true;
			}
		} catch (ServerStoppedException e) {
			System.out.println("Server stopped exception");
			responseStatus = StatusType.SERVER_STOPPED;
			sendResponse = true;
		} catch (ServerNotResponsibleException e) {
			System.out.println("Server not responsible exception");
			responseStatus = StatusType.SERVER_NOT_RESPONSIBLE;
			sendResponse = true;
		} catch (WriteLockException e) {
			System.out.println("Write lock exception");
			responseStatus = StatusType.SERVER_WRITE_LOCK;
			sendResponse = true;
		} catch (KeyNotFoundException e) {
			System.out.println("Key not found exception");
			responseStatus = StatusType.FAILED;
			if (status.equals(StatusType.DELETE)) {
				responseStatus = StatusType.DELETE_ERROR;
			} else if (status.equals(StatusType.GET)) {
				responseStatus = StatusType.GET_ERROR;
			}
			sendResponse = true;
		} catch (Exception e) {
			System.out.println("Unknown exception");
			responseStatus = StatusType.FAILED;
			responseValue = e.getMessage();
			sendResponse = true;
		}

		if (sendResponse) {
			this.sendMessage(new KVM(responseStatus, responseKey, responseValue));
		}
	}

}
