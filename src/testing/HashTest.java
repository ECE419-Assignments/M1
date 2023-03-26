package testing;

import org.junit.Test;

import junit.framework.TestCase;
import shared.metadata.KVMetadata;
import shared.ecs.ECSNode;

public class HashTest extends TestCase {

	@Test
	public void testCorrectHash() {
		KVMetadata metadata = new KVMetadata();
		String key_hash = "3c6e0b8a9c15224a8228b9a98ca1531d";

		String hash = metadata.hashValue("key");
		assertTrue(key_hash.toUpperCase().equals(hash));
	}

	@Test
	public void testKeyRange() {

		KVMetadata metadata = new KVMetadata();
		String expected_key_range = "22C297C6E2FE6938DC31587D555CB79A,3EE36D3770FF83226F5FACF4EB2006FD,localhost:6002;3EE36D3770FF83226F5FACF4EB2006FD,4C084B7A34A16C7F30B85BA107411A63,localhost:6001;4C084B7A34A16C7F30B85BA107411A63,22C297C6E2FE6938DC31587D555CB79A,localhost:6000;";
		metadata.addServer("localhost:6000");
		metadata.addServer("localhost:6001");
		metadata.addServer("localhost:6002");

		String key_range = metadata.getKeyRange();

		assertTrue(expected_key_range.equals(key_range));

	}

	@Test
	public void testKeyRangeToMetadata() {

		KVMetadata metadata = new KVMetadata();
		KVMetadata metadata_two = new KVMetadata();
		metadata.addServer("localhost:6000");
		metadata.addServer("localhost:6001");
		metadata.addServer("localhost:6002");

		String key_range = metadata.getKeyRange();

		metadata_two.createServerTree(key_range);
		String new_key_range = metadata_two.getKeyRange();

		assertTrue(new_key_range.equals(key_range));

	}

	@Test
	public void testSuccessorNode() {
		KVMetadata metadata = new KVMetadata();
		metadata.addServer("localhost:6000");
		metadata.addServer("localhost:6001");
		metadata.addServer("localhost:6002");

		ECSNode successor_6000 = metadata.getSuccesorNode("localhost:6000");
		ECSNode successor_6001 = metadata.getSuccesorNode("localhost:6001");

		assertTrue(successor_6000.getNodeAddress().equals("localhost:6001"));
		assertTrue(successor_6001.getNodeAddress().equals("localhost:6002"));

	}

	@Test
	public void testKeysServer() {
		KVMetadata metadata = new KVMetadata();
		metadata.addServer("localhost:6000");
		metadata.addServer("localhost:6006");

		ECSNode keys_server_1 = metadata.getKeysServer("key");
		ECSNode keys_server_2 = metadata.getKeysServer("key6");

		assertTrue(keys_server_1.getNodeAddress().equals("localhost:6006"));
		assertTrue(keys_server_2.getNodeAddress().equals("localhost:6000"));
	}

	@Test
	public void testMetaDataAddServer() {
		KVMetadata metadata = new KVMetadata();

		metadata.addServer("localhost:6000");
		ECSNode keys_server_1 = metadata.getKeysServer("key");
		assertTrue(keys_server_1.getNodeAddress().equals("localhost:6000"));

		metadata.addServer("localhost:6006");
		keys_server_1 = metadata.getKeysServer("key");
		assertTrue(keys_server_1.getNodeAddress().equals("localhost:6006"));
	}

	@Test
	public void testMetaDataRemoveServer() {
		KVMetadata metadata = new KVMetadata();
		metadata.addServer("localhost:6000");
		ECSNode removed_server = metadata.deleteServer("localhost:6000");
		assertTrue(removed_server.getNodeAddress().equals("localhost:6000"));
	}

	@Test
	public void testReplicaOneServer() {
		KVMetadata metadata = new KVMetadata();
		metadata.addServer("localhost:6000");
		ECSNode[] replicas = metadata.getReplicaNodes("localhost:6000");

		assertNull(replicas[0]);
		assertNull(replicas[1]);
	}

	@Test
	public void testReplicaTwoServers() {
		KVMetadata metadata = new KVMetadata();
		metadata.addServer("localhost:6000");
		metadata.addServer("localhost:6001");
		ECSNode[] replicas = metadata.getReplicaNodes("localhost:6000");

		
		assertNull(replicas[1]);
		assertTrue(replicas[0].getNodeAddress().equals("localhost:6001"));
	}

	@Test
	public void testReplicaThreeServers() {
		KVMetadata metadata = new KVMetadata();
		metadata.addServer("localhost:6000");
		metadata.addServer("localhost:6001");
		metadata.addServer("localhost:6002");
		ECSNode[] replicas = metadata.getReplicaNodes("localhost:6000");

		assertTrue(replicas[0].getNodeAddress().equals("localhost:6001"));
		assertNull(replicas[1].getNodeAddress().equals("localhost:6002"));
	}

	@Test
	public void testReplicaFourServers() {
		KVMetadata metadata = new KVMetadata();
		metadata.addServer("localhost:6000");
		metadata.addServer("localhost:6001");
		metadata.addServer("localhost:6002");
		metadata.addServer("localhost:6003");

		ECSNode[] replicas_6000 = metadata.getReplicaNodes("localhost:6000");
		ECSNode[] replicas_6001 = metadata.getReplicaNodes("localhost:6001");
		ECSNode[] replicas_6002 = metadata.getReplicaNodes("localhost:6002");
		ECSNode[] replicas_6003 = metadata.getReplicaNodes("localhost:6003");

		assertTrue(replicas_6000[0].getNodeAddress().equals("localhost:6001"));
		assertNull(replicas_6000[1].getNodeAddress().equals("localhost:6002"));
		assertTrue(replicas_6001[0].getNodeAddress().equals("localhost:6002"));
		assertNull(replicas_6001[1].getNodeAddress().equals("localhost:6003"));
		assertTrue(replicas_6002[0].getNodeAddress().equals("localhost:6003"));
		assertNull(replicas_6002[1].getNodeAddress().equals("localhost:6000"));
		assertTrue(replicas_6003[0].getNodeAddress().equals("localhost:6000"));
		assertNull(replicas_6003[1].getNodeAddress().equals("localhost:6001"));
	}

	@Test
	public void testIsServerReplicaOf() {
		KVMetadata metadata = new KVMetadata();
		metadata.addServer("localhost:6000");
		metadata.addServer("localhost:6001");

		assertTrue(metadata.isServerReplicaOf("localhost:6000", "localhost6001"));
	}
}
