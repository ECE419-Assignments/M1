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
                this.sendMessage(new KVM(StatusType.UPDATE_METADATA, "", this.ecsClient.kvMetadata.getKeyRange()));
                ServerConnection prevConnection = this.ecsClient
                        .getServerConnectionWithAddress(this.ecsClient.kvMetadata.prevNode(value));

                prevConnection
                        .sendMessage(new KVM(StatusType.TOGGLE_WRITE_LOCK, "", ""));
                Thread.sleep(10);
                prevConnection
                        .sendMessage(new KVM(StatusType.SEND_FILTERED_DATA_TO_NEXT, "", filter_range)); // TODO: Zeni
            } else if (status.equals(StatusType.SERVER_SHUTDOWN)) {
                this.ecsClient.kvMetadata.deleteServer(value);
                this.sendMessage(new KVM(StatusType.TOGGLE_WRITE_LOCK, "", ""));
                ServerConnection prevConnection = this.ecsClient
                        .getServerConnectionWithAddress(this.ecsClient.kvMetadata.prevNode(value));
                prevConnection
                        .sendMessage(new KVM(StatusType.UPDATE_METADATA, "", this.ecsClient.kvMetadata.getKeyRange()));
                Thread.sleep(10);
                this.sendMessage(new KVM(StatusType.SEND_ALL_DATA_TO_PREV, "", prevConnection.address));
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
