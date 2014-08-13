package is.landsbokasafn.deduplicator;

/**
 * These enums correspond to the names of fields in the Lucene index
 */
public enum IndexFields {
    /** The URL 
     *  This value is suitable for use in warc/revisit records as the WARC-Refers-To-Target-URI
     **/
	URL,
    /** The content digest as String **/
	DIGEST,
    /** The URLs timestamp (time of fetch). Suitable for use in WARC-Refers-To-Date. Encoded according to
     *  w3c-iso8601  
     */
    DATE,
    /** The document's etag **/
    ETAG,
    /** A canonicalized version of the URL **/
	URL_CANONICALIZED,
    /** WARC Record ID of original payload capture. Suitable for WARC-Refers-To field. **/
    ORIGINAL_RECORD_ID;

}
