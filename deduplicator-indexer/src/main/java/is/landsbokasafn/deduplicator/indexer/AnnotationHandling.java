package is.landsbokasafn.deduplicator.indexer;

public enum AnnotationHandling {
	NONE,  		      // Do not make any annotations in the crawl.log
	TIMESTAMP,        // Record the original time stamp, do not record the original URL
	TIMESTAMP_AND_URL // Record the original time stamp and the original URL if it differs from the capture URL
	                  // if the URL is the same, simply write - (dash)
}
