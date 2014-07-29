/* CrawlDataItem
 * 
 * Created on 10.04.2006
 *
 * Copyright (C) 2006 National and University Library of Iceland
 * 
 * This file is part of the DeDuplicator (Heritrix add-on module).
 * 
 * DeDuplicator is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser Public License as published by
 * the Free Software Foundation; either version 2.1 of the License, or
 * any later version.
 * 
 * DeDuplicator is distributed in the hope that it will be useful, 
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser Public License
 * along with DeDuplicator; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package is.landsbokasafn.deduplicator;


/**
 * A base class for individual items of crawl data that should be added to the
 * index.
 * 
 * @author Kristinn Sigur&eth;sson
 */
public class CrawlDataItem {
    
    protected String URL;
    protected String originalURL;
    protected int statusCode;
    protected String contentDigest;
    protected String timestamp;
    protected String originalTimestamp;
    protected String etag;
    protected String mimeType;
    protected boolean revisit;
    protected String warcRecordId;
    protected String revisitProfile;
    
    /**
     * Constructor. Creates a new CrawlDataItem with all its data initialized
     * to null.
     */
    public CrawlDataItem(){
    	revisit=false;
    	statusCode=0;
    }
    
    /**
     * Returns the URL
     * @return the URL
     */
    public String getURL() {
        return URL;
    }
    
    /**
     * Set the URL
     * @param URL the new URL
     */
    public void setURL(String URL){
        this.URL = URL;
    }
    
    /**
     * Returns the documents content digest
     * @return the documents content digest
     */
    public String getContentDigest(){
        return contentDigest;
    }
    
    /**
     * Set the content digest
     * @param contentDigest The new value of the content digest
     */
    public void setContentDigest(String contentDigest){
        this.contentDigest = contentDigest;
    }
    
    /**
     * Returns a timestamp for when the URL was fetched in the format consistent with WARC-Date,  w3c-iso8601
     * <p><pre>YYYY-MM-DDThh:mm:ssZ</pre></p>
     * @return the time of the URLs fetching
     */
    public String getTimestamp(){
        return timestamp;
    }
    
    /**
     * Set a new timestamp.
     * @param timestamp The new timestamp. It should be in the format specified for WARC-Date, w3c-iso8601:
     *                  YYYY-MM-DDThh:mm:ssZ
     */
    public void setTimestamp(String timestamp){
        this.timestamp = timestamp;
    }
    
    /**
     * Returns the etag that was associated with the document.
     * <p>
     * If etag is unavailable null will be returned. 
     * @return the etag.
     */
    public String getEtag(){
        return etag;
    }
    
    /**
     * Set a new Etag
     * @param etag The new etag
     */
    public void setEtag(String etag){
        this.etag = etag;
    }

    /**
     * Returns the mimetype that was associated with the document.
     * @return the mimetype.
     */
    public String getMimeType(){
        return mimeType==null?"unknown":mimeType;
    }
    
    /**
     * Set new MIME type.
     * @param mimeType The new MIME type
     */
    public void setMimeType(String mimeType){
        this.mimeType = mimeType;
    }

    /**
     * Returns whether the CrawlDataItem represents a revisit
     * @return true if revisit, false otherwise
     */
    public boolean isRevisit() {
        return revisit;
    }
    
    /**
     * Set whether revisit or not.
     * @param revisit true if revisit, false otherwise
     */
    public void setRevisit(boolean revisit) {
        this.revisit = revisit;
    }

	/**
	 * Get the HTTP (or Heritrix if 0 or smaller) status code associated with the item
	 * @return
	 */
	public int getStatusCode() {
		return statusCode;
	}

	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}

	public String getWarcRecordId() {
		return warcRecordId;
	}

	public void setWarcRecordId(String warcRecordId) {
		this.warcRecordId = warcRecordId;
	}
	
	public String getOriginalURL() {
		return originalURL;
	}

	public void setOriginalURL(String originalURL) {
		this.originalURL = originalURL;
	}

	public String getOriginalTimestamp() {
		return originalTimestamp;
	}

	public void setOriginalTimestamp(String originalTimestamp) {
		this.originalTimestamp = originalTimestamp;
	}
	
	
	public String getRevisitProfile() {
		return revisitProfile;
	}

	public void setRevisitProfile(String revisitProfile) {
		this.revisitProfile = revisitProfile;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("URL: ");
		sb.append(URL);
		sb.append("\nOriginal URL: ");
		sb.append(originalURL);
		sb.append("\nTimestamp: ");
		sb.append(timestamp);
		sb.append("\nOriginal timestamp: ");
		sb.append(originalTimestamp);
		sb.append("\nDigest: ");
		sb.append(contentDigest);
		sb.append("\nMimeType: ");
		sb.append(mimeType);
		sb.append("\nRevisit: ");
		sb.append(revisit);
		sb.append("\nStatusCode: ");
		sb.append(statusCode);
		sb.append("\nE-Tag: ");
		sb.append(etag);
		sb.append("\nWARC Record-ID: ");
		sb.append(warcRecordId);
		sb.append("\nWARC Profile: ");
		sb.append(revisitProfile);
		
		return sb.toString();
	}
}
