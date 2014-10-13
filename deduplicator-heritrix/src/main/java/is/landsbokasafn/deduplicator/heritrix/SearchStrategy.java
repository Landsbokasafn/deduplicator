package is.landsbokasafn.deduplicator.heritrix;

public enum SearchStrategy {
	/**
	 * The URL of the original record must match the current one exactly. Requires only indexing on URL, 
	 * we scan through the digests found to see if any match the current one. If DIGEST is also indexed, 
	 * a more targeted query can be made. DIGEST index will be used if available and is recommended if URLs 
	 * are commonly indexed with multiple different digests.
	 */
	URL_EXACT,
	
	/** 
	 * A search on canonical URLs if they are available in the index. No attempt is made to find an exact URL match. 
	 * Any canonical match is deemed good enough. Only the URL fields need to be indexed and canonical URL must be 
	 * in the index. DIGEST index will be used if available and is recommended if URLs are commonly indexed with 
	 * multiple different digests.
	 */
	URL_CANONICAL,
	
	/**
	 * Do a search on the DIGEST field only. Any hit is a valid duplicate. Only requires that the DIGEST be indexed.
	 */
	DIGEST_ANY,

}
