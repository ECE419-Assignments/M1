import java.io.IOException;

import org.apache.log4j.Level;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import client.KVStore;
import app_kvServer.IKVServer.CacheStrategy;
import logger.LogSetup;

public class performance_tests extends Thread {

    private KVStore kvClient;

    public performance_tests(int nmb_servers, int nmb_clients, int cacheSize, int base_port) {
        try {
            new LogSetup("logs/testing/test.log", Level.ERROR);
            new ECSClient(51000);
            Thread.sleep(1000);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < nmb_servers; i++) {
            new KVServer(base_port + i, cacheSize, CacheStrategy.FIFO, "localhost", 51000);
        }

        for (int i = 0; i < nmb_clients; i++) {
            this.start();
        }
    }

    public void run() {
    }
}
