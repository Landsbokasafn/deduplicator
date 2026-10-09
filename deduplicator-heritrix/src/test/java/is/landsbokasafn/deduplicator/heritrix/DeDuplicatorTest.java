package is.landsbokasafn.deduplicator.heritrix;

import junit.framework.TestCase;

import org.archive.modules.CrawlURI;
import org.archive.modules.CrawlURI.FetchType;
import org.archive.net.UURIFactory;

public class DeDuplicatorTest extends TestCase {

	public void testGetPercentage() throws Exception{
		assertEquals("2.5%",DeDuplicator.getPercentage(5,200));
	}

	public void testShouldProcessRequiresContentDigest() throws Exception {
		DeDuplicator deduplicator = new DeDuplicator();
		CrawlURI curi = new CrawlURI(UURIFactory.getInstance("http://example.is/"));
		curi.setFetchType(FetchType.HTTP_GET);
		curi.setFetchStatus(200);

		// Heritrix 3.18+ leaves the digest empty when a chunked response can't be decoded
		assertFalse(deduplicator.shouldProcess(curi));

		curi.setContentDigest("sha1", new byte[20]);
		assertTrue(deduplicator.shouldProcess(curi));
	}

}
