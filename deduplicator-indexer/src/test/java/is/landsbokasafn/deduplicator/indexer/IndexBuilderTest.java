package is.landsbokasafn.deduplicator.indexer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.FSDirectory;

public class IndexBuilderTest extends TestCase {

	public void testItemsWithoutDigestAreSkipped() throws IOException {
		File indexDir = Files.createTempDirectory("dedup-index").toFile();
		try {
			IndexBuilder builder = new IndexBuilder(indexDir.getAbsolutePath(), true, true, false, false, false);
			long count = builder.writeToIndex(new ListIterator(
					item("http://example.is/a.pdf", "sha1:YA3G7O6TNMHXA5WWDSIZJDNXV56WDRCA"),
					item("http://example.is/b.pdf", null),  // no WARC-Payload-Digest header
					item("http://example.is/c.pdf", "-")),  // crawl.log with no digest
					"^text/.*", true, false);
			builder.close();

			assertEquals(1, count);
			DirectoryReader reader = DirectoryReader.open(FSDirectory.open(indexDir));
			try {
				assertEquals(1, reader.numDocs());
			} finally {
				reader.close();
			}
		} finally {
			FileUtils.deleteQuietly(indexDir);
		}
	}

	private static CrawlDataItem item(String url, String digest) {
		CrawlDataItem item = new CrawlDataItem();
		item.setURL(url);
		item.setContentDigest(digest);
		item.setTimestamp("2026-10-09T10:00:00Z");
		item.setStatusCode(200);
		item.setMimeType("application/pdf");
		item.setRevisit(false);
		return item;
	}

	private static class ListIterator implements CrawlDataIterator {
		private final Iterator<CrawlDataItem> items;

		ListIterator(CrawlDataItem... items) {
			this.items = Arrays.asList(items).iterator();
		}

		public void initialize(String source) {
		}

		public boolean hasNext() {
			return items.hasNext();
		}

		public CrawlDataItem next() {
			return items.next();
		}

		public void close() {
		}

		public String getSourceType() {
			return "In-memory test items";
		}
	}

}
