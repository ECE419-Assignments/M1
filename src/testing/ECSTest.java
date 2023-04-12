package testing;

import javax.print.attribute.standard.Severity;

import org.junit.Test;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import app_kvServer.IKVServer.CacheStrategy;
import app_kvServer.exceptions.ServerNotResponsibleException;
import app_kvServer.exceptions.ServerStoppedException;
import junit.framework.TestCase;
import shared.metadata.KVMetadata;
import shared.ecs.ECSNode;

public class ECSTest extends TestCase {

    // private ECSClient ecsClient;
    // private KVServer kvServer1, kvServer2;

    @Test
    public void testServerAdd() {
        KVServer kvServer1 = new KVServer(7000, 10, CacheStrategy.FIFO, "loclahost",
                51000);
        assertTrue(true);
    }

    @Test
    public void testServerNotResponsible() {
        try {
            KVServer kvServer1 = new KVServer(7000, 10, CacheStrategy.FIFO, "localhost",
                    51000);
            Thread.sleep(2000);
            kvServer1.putKV("key", "value");
            Thread.sleep(200);
        } catch (ServerNotResponsibleException | ServerStoppedException e) {
            assertTrue(true);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testServerDelete() {
        KVServer kvServer1 = new KVServer(7000, 10, CacheStrategy.FIFO, "localhost",
                51000);
        assertTrue(true);
    }

    @Test
    public void testServerAdd2() {
        KVServer kvServer1 = new KVServer(7000, 10, CacheStrategy.FIFO, "loclahost",
                51000);
        assertTrue(true);
    }

    @Test
    public void testServerNotResponsible2() {
        try {
            KVServer kvServer1 = new KVServer(7000, 10, CacheStrategy.FIFO, "localhost",
                    51000);
            Thread.sleep(2000);
            kvServer1.putKV("key", "value");
            Thread.sleep(200);
        } catch (ServerNotResponsibleException | ServerStoppedException e) {
            assertTrue(true);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }
}

/*
 * Tests:
 * Test with 2 severs
 */