package is.landsbokasafn.deduplicator.heritrix;

public enum SearchStrategy {
	/**
	 * Do a search where the URL and digest must both match. The URL field must be indexed. 
	 */
	URL_EXACT,
	
	/** 
	 * A search on canonical URLs if they are available in the index. No attempt is made to find an exact URL match. 
	 * Any canonical match is deemed good enough. Only the URL fields need to be indexed and canonical URL must be 
	 * in the index. 
	 */
	URL_CANONICAL,
	
	/**
	 * Do a search on the DIGEST field, but include criteria that will cause documents where the URL also matches
	 * to be more likely to be the first in the return results. This makes is much more likely that an exact URL
	 * match will be made when it is possible. It does <strong>not</strong> guarantee it.  Note, that this is
	 * significantly slower than {@link #DIGEST_ANY}. Requires that the URL field be indexed (otherwise it runs
	 * the same as {@link #DIGEST_ANY}).
	 */
	DIGEST_URL_PREFERRED,
	
	/**
	 * Do a search on the DIGEST field only. Any hit is a valid duplicate. Only requires that the DIGEST be indexed.
	 */
	DIGEST_ANY,

}
