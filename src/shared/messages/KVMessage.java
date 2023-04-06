package shared.messages;

public interface KVMessage {

	public enum StatusType {
		GET, /* Get - request */
		GET_ERROR, /* requested tuple (i.e. value) not found */
		GET_SUCCESS, /* requested tuple (i.e. value) found */
		PUT, /* Put - request */
		PUT_SUCCESS, /* Put - request successful, tuple inserted */
		PUT_UPDATE, /* Put - request successful, i.e. value updated */
		PUT_REPLICA,
		PUT_REPLICA_SUCCESS,
		PUT_ERROR, /* Put - request not successful */
		DELETE, /* Delete -request */
		DELETE_REPLICA,
		DELETE_REPLICA_SUCCESS,
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR, /* Delete - request successful */
		MESSAGE, /* Sending a message back to client */
		FAILED,
		SERVER_NOT_RESPONSIBLE,
		SERVER_WRITE_LOCK,
		SERVER_STOPPED,
		KEYRANGE,
		GET_KEYRANGE,
		GET_KEYRANGE_SUCCESS,

		START_SERVER,

		NEW_SERVER,
		SERVER_SHUTDOWN,
		UPDATE_METADATA,
		TOGGLE_WRITE_LOCK,
		TOGGLE_WRITE_LOCK_SUCCESS,
		SEND_FILTERED_DATA_TO_NEXT,
		SEND_ALL_DATA_TO_PREV,
		DATA_MOVED_CONFIRMATION_SHUTDOWN,
		DATA_MOVED_CONFIRMATION_NEW,
		CLOSE_LAST_SERVER,

		UPDATE_REPLICAS,

		MOVE_REPLICA_TO_MAIN_CACHE,
		MOVE_REPLICA_TO_MAIN_CACHE_SUCCESS,
		MOVE_REPLICA_TO_MAIN_CACHE_FAIL,

		NEW_ECS,
		SET_BACKUP_ECS_ADDRESS,
		NEW_SERVER_CONNECTING_TO_BACKUP_ECS
	}

	/**
	 * @return the key that is associated with this message,
	 *         null if not key is associated.
	 */
	public String getKey();

	/**
	 * @return the value that is associated with this message,
	 *         null if not value is associated.
	 */
	public String getValue();

	/**
	 * @return a status string that is used to identify request types,
	 *         response types and error types associated to the message.
	 */
	public StatusType getStatus();

}
