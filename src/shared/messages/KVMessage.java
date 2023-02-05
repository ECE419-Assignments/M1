package shared.messages;

import java.nio.charset.StandardCharsets;

public interface KVMessage {

	public enum StatusType {
		GET, /* Get - request */
		GET_ERROR, /* requested tuple (i.e. value) not found */
		GET_SUCCESS, /* requested tuple (i.e. value) found */
		PUT, /* Put - request */
		PUT_SUCCESS, /* Put - request successful, tuple inserted */
		PUT_UPDATE, /* Put - request successful, i.e. value updated */
		PUT_ERROR, /* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR /* Delete - request successful */
	}

	String key = null;
	String value = null;
	StatusType status = null;

	public static void msgDecoder(byte[] msgBytes) {
		String decodedMessage = new String(msgBytes, StandardCharsets.US_ASCII);
		String[] parts = decodedMessage.split(":");

		// if (parts.length != 3) {
		// throw new IllegalArgumentException("Encoded message does not have the correct
		// format");
		// }

		String key = parts[0];
		String value = parts[1];
		StatusType status = StatusType.valueOf(parts[2]);

		if (!key.matches("[a-zA-Z]+") || !value.matches("[a-zA-Z]+")) {
			throw new IllegalArgumentException("Key or value contain characters outside of [a-zA-Z]");
		}
	}

	/**
	 * @return the key that is associated with this message,
	 *         null if not key is associated.
	 */
	public default String getKey() {
		return key;
	}

	/**
	 * @return the value that is associated with this message,
	 *         null if not value is associated.
	 */
	public default String getValue() {
		return value;
	}

	/**
	 * @return a status string that is used to identify request types,
	 *         response types and error types associated to the message.
	 */
	public default StatusType getStatus() {
		return status;
	}
}