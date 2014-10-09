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
	 * Same as {@link #URL_EXACT} except that if no hit is found then proceed to run a search that is 
	 * equivalent to {@link #URL_CANONICAL}. This requires two searches of the index. Only the
	 * URL fields need to be indexed. DIGEST index will be used if available and is recommended if URLs are 
	 * commonly indexed with multiple different digests.
	 */
	URL_CANONICAL_FALLBACK,
	
	/**
	 * Do a search on the DIGEST field only. Any hit is a valid duplicate. Only requires that the DIGEST be indexed.
	 */
	DIGEST_ANY,

	/**
	 * Same as {@link #URL_CANONICAL_FALLBACK} except that if no hit is found then proceed to run a search that is
	 * equivalent to {@link #DIGEST_ANY}. This can require up to three searches in the index per URL. 
	 * Both URL and DIGEST fields must be indexed.
	 */
	URL_DIGEST_FALLBACK,
}
