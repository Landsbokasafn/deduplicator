package is.landsbokasafn.deduplicator.heritrix;

public interface Index {

	/**
	 * 
	 * @param url The URL (as extracted from source) of the resource. 
	 * @param canonicalizedURL The canonicalized form of the URL.
	 * @param digest The contents digest of the URL based on the latest network capture. 
	 * 
	 * @return A {@link Duplicate} object if a duplicate is found in the index. Otherwise, returns null.
	 */
	Duplicate lookup(String url, String canonicalizedURL, String digest);
	
	SearchStrategy getSearchStrategy();

}
