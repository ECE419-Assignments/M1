package shared.messages;

public interface KVMessage {
	
	public enum StatusType {
		GET, 			/* Get - request */
		GET_ERROR, 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found */
		PUT, 			/* Put - request */
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR 	/* Delete - request successful */
	}


	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	// public String getKey();
	public default String getKey(String message) {
        String[] parts = message.split(":");
        if (parts.length >= 2) {
            String key = parts[1];
            if (key.matches("[a-zA-Z]+")) {
                return key;
            } else {
                throw new IllegalArgumentException("Key must contain only letters");
            }
        }
        return null;
    }
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	// public String getValue();
	public default String getValue(String message) {
        String[] parts = message.split(":");
        if (parts.length >= 3) {
            String value = parts[2];
            if (value.matches("[a-zA-Z]+")) {
                return value;
            } else {
                throw new IllegalArgumentException("Value must contain only letters");
            }
        }
        return null;
    }
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	// public StatusType getStatus();
	public default StatusType getStatus(String message) {
		String[] parts = message.split(":");
		if (parts.length < 1) {
			return null;
		}
		try {
			return StatusType.valueOf(parts[0]);
		} catch (IllegalArgumentException e) {
			return null;
		}
	}
	
}