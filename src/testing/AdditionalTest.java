package testing;

import org.junit.Test;

import junit.framework.TestCase;
import shared.metadata.KVMetadata;

public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 3
	
	@Test
	public void testCorrectHash() {
		KVMetadata metadata = new KVMetadata();
		String key_hash = "3c6e0b8a9c15224a8228b9a98ca1531d";

		String hash = metadata.hashValue("key");
		System.out.println(hash);
		assertTrue(key_hash.toUpperCase().equals(hash));
	}
}
