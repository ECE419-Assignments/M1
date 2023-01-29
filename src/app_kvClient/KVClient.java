package app_kvClient;

import app_kvServer.KVServer;
import client.KVCommInterface;
import client.KVStore;

import logger.LogSetup;
import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KVClient implements IKVClient {

    private static Logger logger = Logger.getRootLogger();

    private KVStore kvStore;
    private String hostname;
    private int port;

    @Override
    public void newConnection(String hostname, int port) throws Exception {
        this.hostname = hostname;
        this.port = port;
        this.getStore().connect();
        // TODO Auto-generated method stub
        // create KVComminterface -> Connect to hostname and
    }

    @Override
    public KVCommInterface getStore() {
        if (this.kvStore == null) {
            this.kvStore = new KVStore(this.hostname, this.port);
        }
        return this.kvStore;
    }

    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.ALL);
            if (args.length != 2) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: Server <hostname> <port>!");
            } else {
                String hostname = args[0];
                int port = Integer.parseInt(args[1]);
                new KVClient().newConnection(hostname, port);
            }
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) {
            System.out.println("Error! Invalid argument <port>! Not a number!");
            System.out.println("Usage: Client <hostname> <port>!");
            System.exit(1);
        } catch (Exception e) {
            System.out.println("Unknown Error!");
            System.out.println("Usage: Client <hostname> <port>!");
            System.exit(1);
        }

    }
}
