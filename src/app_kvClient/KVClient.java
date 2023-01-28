package app_kvClient;

import client.KVCommInterface;

public class KVClient implements IKVClient {
    private KVStore kvStore;

    @Override
    public void newConnection(String hostname, int port) throws Exception{
        getStore().connect();
        // TODO Auto-generated method stub
        // create KVComminterface -> Connect to hostname and
    }

    @Override
    public KVCommInterface getStore(){
        if (!kvStore) {
            kvStore = new KVStore(hostname, port);
        }
        return kvStore;
    }
}

/*
 * TODO: Needs some sort of run method that is constanly checking for a return from the server through the socket.
 * TODO: Nees some sort of class that parses the CLI.
 * TODO: 
 */
