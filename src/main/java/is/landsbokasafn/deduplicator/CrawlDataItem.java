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
    protected int statusCode;
    protected String contentDigest;
    protected String timestamp;
    protected String etag;
    protected String mimeType;
    protected boolean revisit;
    protected long size;
    protected String warcRecordId;
    
    /**
     * Constructor. Creates a new CrawlDataItem with all its data initialized
     * to null.
     */
    public CrawlDataItem(){
    	revisit=false;
    	statusCode=0;
        size = -1;
    }
    
    /**
     * Constructor. Creates a new CrawlDataItem with all its data initialized
     * via the constructor.
     * 
     * @param URL The URL for this CrawlDataItem
     * @param contentDigest A content digest of the document found at the URL
     * @param timestamp Date of when the content digest was valid for that URL. 
     *                  Format: yyyyMMddHHmmssSSS
     * @param etag Etag for the URL
     * @param mimeType MIME type of the document found at the URL
     * @param origin The origin of the CrawlDataItem (the exact meaning of the
     *               origin is outside the scope of this class and it may be
     *               any String value)
     * @param revisit True if this CrawlDataItem was marked as duplicate
     */
    public CrawlDataItem(String URL, String contentDigest, String timestamp, String etag, String mimeType, 
    		boolean revisit, long size, int statusCode, String warcRecordId){
        this.URL = URL;
        this.contentDigest = contentDigest;
        this.timestamp = timestamp;
        this.etag = etag;
        this.mimeType = mimeType;
        this.revisit = revisit;
        this.size = size;
        this.statusCode = statusCode;
        this.warcRecordId = warcRecordId;
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
        return mimeType;
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
     * Get the size of the CrawlDataItem. 
     * @return The size or -1 if the size could not be determined.
     */
	public long getSize() {
		return size;
	}

	/**
	 * Set the size of the CrawlDataItem
	 * @param size The size or -1 if the size is indeterminate
	 */
	public void setSize(long size) {
		this.size = size;
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

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("URL: ");
		sb.append(URL);
		sb.append("Timestamp: ");
		sb.append(timestamp);
		sb.append("\nDigest: ");
		sb.append(contentDigest);
		sb.append("\nMimeType: ");
		sb.append(mimeType);
		sb.append("\nRevisit: ");
		sb.append(revisit);
		sb.append("\nSize: ");
		sb.append(size);
		sb.append("\nStatusCode: ");
		sb.append(statusCode);
		sb.append("\nE-Tag: ");
		sb.append(etag);
		sb.append("\nWARC Record-ID:");
		sb.append(warcRecordId);
		
		return sb.toString();
	}
}
