package client;
import shared.messages.KVM;

public interface ClientSocketListener {

	public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};
	
	public void handleNewMessage(KVM msg);
	
	public void handleStatus(SocketStatus status);
}
