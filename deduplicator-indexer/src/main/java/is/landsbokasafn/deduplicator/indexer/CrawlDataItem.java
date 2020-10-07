/* Copyright (C) 2006-2014 National and University Library of Iceland (NULI)
 * 
 * This file is part of the DeDuplicator (Heritrix add-on module).
 * 
 *  NULI licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package is.landsbokasafn.deduplicator.indexer;


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
     * Returns the documents content digest (including prefix identifying the hashing algorithm)
     * @return the documents content digest
     */
    public String getContentDigest(){
        return contentDigest;
    }
    
    /**
     * Set the content digest
     * @param contentDigest The new value of the content digest (including prefix identifying the hashing algorithm)
     */
    public void setContentDigest(String contentDigest){
        this.contentDigest = contentDigest;
    }
    
    /**
     * Returns a timestamp for when the URL was fetched in the format consistent with WARC-Date,  w3c-iso8601
     * <pre>YYYY-MM-DDThh:mm:ssZ</pre>
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
	 * @return the status code
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
