package app_kvECS;

import java.io.IOException;
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
            if (status.equals(StatusType.NEW_SERVER)) {
                this.ecsClient.kvMetadata.addServer(value);
                responseStatus = StatusType.TOGGLE_WRITE_LOCK;

                ServerConnection connection = this.ecsClient
                        .getServerConnectionWithAddress(this.ecsClient.kvMetadata.prevNode());
                connection
                        .sendMessage(new KVM(StatusType.UPDATE_METADATA, "", this.ecsClient.kvMetadata.getKeyRange()));
                sendResponse = true;
            } else if (status.equals(StatusType.DATA_MOVED_CONFIRMATION)) {
                this.ecsClient.updateAllServerMetadatas();
                sendResponse = false;
            }
        } catch (Exception e) {
            responseStatus = StatusType.FAILED;
            responseValue = e.getMessage();
        }

        if (sendResponse) {
            this.sendMessage(new KVM(responseStatus, responseKey, responseValue));
        }
    }

}
