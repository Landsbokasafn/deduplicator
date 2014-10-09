package is.landsbokasafn.deduplicator.heritrix;

public enum DuplicateType {
	EXACT_URL, 		// Duplicate where the URL and digest both match
	CANONICAL_URL,  // Duplicate where the URL's canonical forms match as well as the digest
	DIGEST_ONLY          // Duplicate based on digest only. URLs do not correlate
}
