package is.landsbokasafn.deduplicator.heritrix;

import org.archive.modules.revisit.IdenticalPayloadDigestRevisit;

public interface Index {

	/**
	 * 
	 * @param url The URL (as extracted from source) of the resource. 
	 * @param canonicalizedURL The canonicalized form of the URL.
	 * @param digest The contents digest of the URL based on the latest network capture. 
	 * 
	 * @return An {@link IdenticalPayloadDigestRevisit} object if a duplicate is found in the index. 
	 *         Otherwise, returns null.
	 */
	IdenticalPayloadDigestRevisit lookup(String url, String canonicalizedURL, String digest);
	
	String getInfo();

}
