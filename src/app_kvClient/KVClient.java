package app_kvClient;

import client.KVCommInterface;
import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KVClient implements IKVClient {
    
	private static Logger logger = Logger.getRootLogger();
    private KVStore kvStore

    @Override
    public void newConnection(String hostname, int port) throws Exception{
        getStore().connect();
        logger.info("Connected to " 
	                		+ client.getInetAddress().getHostName() 
	                		+  " on port " + client.getPort());
        // TODO Auto-generated method stub
        // create KVComminterface -> Connect to hostname and
    }

    @Override
    public KVCommInterface getStore(String[] args){
        if (!kvStore) {
            kvStore = new KVStore(hostname, port);
        }
        return kvStore;
    }

    public void main() {
        new LogSetup("logs/client.log", Level.ALL);
        if(args.length != 2) {
            System.out.println("Error! Invalid number of arguments!");
            System.out.println("Usage: Server <hostname> <port>!");
        } else {
            String hostname = args[0];
            int port = Integer.parseInt(args[1]);
            newConnection(hostname, port);
            //TODO: Replace to use threading
            // new KVServer(port, 10, CacheStrategy.None).start();
            new KVServer(port, 10, "None").run();
        }
    }
}
