package app_kvServer;

import java.net.Socket;

import app_kvServer.exceptions.KeyNotFoundException;
import app_kvServer.exceptions.ServerNotResponsibleException;
import app_kvServer.exceptions.ServerStoppedException;
import app_kvServer.exceptions.WriteLockException;
import shared.BaseConnection;
import shared.messages.KVM;
import shared.messages.KVMessage.StatusType;

public class ClientConnection extends BaseConnection {

	private KVServer kvServer;

	public ClientConnection(KVServer kvServer, Socket socket) {
		super(socket);
		this.kvServer = kvServer;
	}

	@Override()
	public KVM processMessage(KVM message) {
		StatusType status = message.getStatus();
		String key = message.getKey();
		String value = message.getValue();
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
				value = this.kvServer.getKV(message.getKey());
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
		return new KVM(responseStatus, responseKey, responseValue);
	}

}
