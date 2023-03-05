package app_kvECS;

import java.net.Socket;

import app_kvServer.exceptions.KeyNotFoundException;
import app_kvServer.exceptions.ServerNotResponsibleException;
import app_kvServer.exceptions.ServerStoppedException;
import app_kvServer.exceptions.WriteLockException;
import shared.BaseConnection;
import shared.messages.KVM;
import shared.messages.KVMessage.StatusType;

public class ServerConnection extends BaseConnection {

    private ECSClient ecsClient;

    public ServerConnection(ECSClient ecsClient, Socket socket) {
        super(socket);
        this.ecsClient = ecsClient;
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
        boolean sendResponse = true;

        try {
            if (status.equals(StatusType.PUT)) {

            } else if (status.equals(StatusType.GET)) {

            }
        } catch (Exception e) {
            responseStatus = StatusType.FAILED;
            responseValue = e.getMessage();
        }
        return new KVM(responseStatus, responseKey, responseValue);
    }

}
