package shared;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import shared.messages.KVM;
import shared.messages.KVMessage.StatusType;

public class BaseConnection implements Runnable {

    protected Logger logger = Logger.getLogger("Client Connection");

    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket socket;
    private InputStream input;
    private OutputStream output;
    private KVM latestMsg;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     * 
     * @param socket the Socket object for the client connection.
     */

    public void postStart() throws IOException {
    }

    public BaseConnection(String host, int port) {
        try {
            this.socket = new Socket(host, port);
            this.isOpen = true;
        } catch (Exception e) {
            System.out.println("error connecting to other host and port");
        }
    }

    public BaseConnection(Socket socket) {
        this.socket = socket;
        this.isOpen = true;
    }

    public void close() {
        this.isOpen = false;
    }

    public void processMessage(KVM message) throws IOException {
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {
            output = socket.getOutputStream();
            input = socket.getInputStream();

            // TODO: CHANGE FROM EMPTY SPACE TO SOMETHING ELSE
            sendMessage(new KVM(StatusType.MESSAGE, " ",
                    "Connection to process established "
                            + socket.getLocalAddress() + " "
                            + socket.getLocalPort()));

            postStart();

            // TODO: More informative logs on server side.
            while (isOpen) {
                try {
                    KVM latestMsg = receiveMessage();
                    this.processMessage(latestMsg);
                } catch (IOException ioe) {
                    System.out.println(ioe);
                    logger.error("Error! Connection lost!", ioe);
                    isOpen = false;
                } catch (NumberFormatException e) {
                    isOpen = false;
                } catch (Exception e) {
                    logger.error("Error! Connection lost!", e);
                    isOpen = false;
                }
            }

        } catch (IOException ioe) {
            logger.error("Error! Connection could not be established!");

        } finally {

            try {
                if (socket != null) {
                    input.close();
                    output.close();
                    socket.close();
                }
            } catch (IOException ioe) {
                logger.error("Error! Unable to tear down connection!");
            }
        }
    }

    public KVM getLatestMsg() {
        return latestMsg;
    }

    /**
     * Method sends a message using this socket.
     * 
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendMessage(KVM msg) throws IOException {
        byte[] msgBytes = msg.getMsgBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info("SEND \t<"
                + socket.getInetAddress().getHostAddress() + ":"
                + socket.getPort() + ">: '"
                + msg.getMsg() + "'");
    }

    private KVM receiveMessage() throws IOException, Exception {

        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        /* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

        while (/* read != 13 && */ read != 10 && read != -1 && reading) {/* CR, LF, error */
            /* if buffer filled, copy to msg array */
            if (index == BUFFER_SIZE) {
                if (msgBytes == null) {
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                            BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

            /* only read valid characters, i.e. letters and constants */
            bufferBytes[index] = read;
            index++;

            /* stop reading is DROP_SIZE is reached */
            if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

            /* read next char from stream */
            read = (byte) input.read();
        }

        if (msgBytes == null) {
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }

        msgBytes = tmp;

        /* build final String */
        KVM msg = new KVM(msgBytes);
        logger.info("RECEIVE \t<"
                + socket.getInetAddress().getHostAddress() + ":"
                + socket.getPort() + ">: '"
                + msg.getMsg().trim() + "'");
        return msg;
    }

}
