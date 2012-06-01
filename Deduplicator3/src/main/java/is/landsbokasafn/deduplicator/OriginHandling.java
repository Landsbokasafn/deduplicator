package is.landsbokasafn.deduplicator;

public enum OriginHandling {
	NONE,  		// No origin information
	PROCESSOR,  // Use processor setting -- ATTR_ORIGIN
	INDEX       // Use index information, each hit on index should contain origin
}
