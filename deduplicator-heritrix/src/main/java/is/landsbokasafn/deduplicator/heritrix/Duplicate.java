package is.landsbokasafn.deduplicator.heritrix;

/**
 * Represents a discovered duplicate 
 */
public class Duplicate {
	String url;
	String date; // In w3c-iso8601 form
	DuplicateType type;
	String WarcRecordId;
	
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public DuplicateType getType() {
		return type;
	}
	public void setType(DuplicateType type) {
		this.type = type;
	}
	public String getWarcRecordId() {
		return WarcRecordId;
	}
	public void setWarcRecordId(String warcRecordId) {
		WarcRecordId = warcRecordId;
	}
	
	
}
