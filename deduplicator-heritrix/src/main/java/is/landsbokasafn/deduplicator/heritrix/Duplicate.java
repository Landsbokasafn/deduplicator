package is.landsbokasafn.deduplicator.heritrix;

/**
 * Represents a discovered duplicate 
 */
public class Duplicate {
	String url;
	String date; // In w3c-iso8601 form
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
	public String getWarcRecordId() {
		return WarcRecordId;
	}
	public void setWarcRecordId(String warcRecordId) {
		WarcRecordId = warcRecordId;
	}
	
	
}
