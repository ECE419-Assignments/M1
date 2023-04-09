package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import app_kvServer.IKVServer.CacheStrategy;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;

public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
			new ECSClient(51000, "", false);
			new KVServer(50000, 10, CacheStrategy.FIFO, "127.0.0.1", 51000);
			Thread.sleep(1000);
			Thread.sleep(1000);
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		// clientSuite.addTestSuite(ConnectionTest.class);
		// clientSuite.addTestSuite(InteractionTest.class);
		// clientSuite.addTestSuite(AdditionalTest.class);
		// clientSuite.addTestSuite(ECSTest.class);
		// clientSuite.addTestSuite(HashTest.class);
		clientSuite.addTestSuite(ECSBackupTest.class);
		return clientSuite;
	}

}
