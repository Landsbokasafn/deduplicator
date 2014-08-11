package is.landsbokasafn.deduplicator;

import java.io.IOException;

public interface IdenticalPayloadDigestRevisitResolver {

	/**
	 * Finds an original (non-revisit) record for the supplied URL and digest. If there are multiple possible 
	 * records matching the supplied data, it will return a match as close in time as possible. When digest only
	 * matches are allowed, url and digest matches are always preferred over digest only matches.
	 * Under no circumstances will it return a record made after <pre>revisitTime</pre>.
	 * 
	 * @param url The URL of the revisit entry to resolve. May be null if <pre>allowDigestOnly</p> is true.
	 * @param digest The content digest of the revisit entry to resolve. This value may not be null.
	 * @param revisitTime The time of capture for this revisit, encoded according to w3c-iso8601
	 * @param allowDigestOnly If false, digest matches on different URLs will never be considered. IF true, they
	 *                        will be considered if no url+digest matches are found.
	 * @return A {@link CrawlDataItem} representing a WARC response record with the same digest and created prior
	 *         to the specified revisit time.
	 * @throws IOException 
	 */
	public CrawlDataItem resolve(String url, String digest, String revisitTime, boolean allowDigestOnly) 
			throws IOException;
}
