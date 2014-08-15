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

import static is.landsbokasafn.deduplicator.DeDuplicatorConstants.REVISIT_ANNOTATION_MARKER;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.NoSuchElementException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.archive.util.DateUtils;

/**
 * An implementation of a  {@link is.hi.bok.deduplicator.CrawlDataIterator}
 * capable of iterating over a Heritrix's style <code>crawl.log</code>.
 * 
 * @author Kristinn Sigur&eth;sson
 * @author Lars Clausen
 */
public class CrawlLogIterator implements CrawlDataIterator {
	private static final Log log = LogFactory.getLog(CrawlLogIterator.class);

	// By default, we look for the standard revisit annotation marker 
	// to indicate that a crawl line represents a revisit
	private static final String REVISIT_ANNOTATION_REGEX = "^.*"+REVISIT_ANNOTATION_MARKER + ".*$";
	private static final String REVISIT_ANNOTATION_REGEX_PROPERTY = 
			"deduplicator.crawllogiterator.revisit-annotation-regex";
	
	private static String revisitMatchingRegex;
	
    /** 
     * A reader for the crawl.log file being processed
     */
    protected BufferedReader in;
    
    /**
     * The next item to be issued (if ready) or null if the next item
     * has not been prepared or there are no more elements 
     */
    protected CrawlDataItem next;
    
    public CrawlLogIterator() {
    	revisitMatchingRegex = System.getProperty(REVISIT_ANNOTATION_REGEX_PROPERTY);
    	if (revisitMatchingRegex==null) {
    		revisitMatchingRegex=REVISIT_ANNOTATION_REGEX;
    	}
    }
    
    /** 
     * Create a new CrawlLogIterator that reads items from a Heritrix crawl.log
     *
     * @param source The path of a Heritrix crawl.log file.
     * @throws IOException If errors were found reading the log.
     */
    public void initialize(String source) throws IOException {
    	log.info("Opening " + source);
        in = new BufferedReader(new InputStreamReader(
                new FileInputStream(new File(source))));
    }

    /** 
     * Returns true if there are more items available.
     *
     * @return True if at least one more item can be fetched with next().
     */
    public boolean hasNext() throws IOException {
        if(next == null){
            prepareNext();
        }
        return next!=null;
    }

    /** 
     * Returns the next valid item from the crawl log.
     *
     * @return An item from the crawl log.  
     * @throws IOException If there is an error reading the item *after* the
     *         item to be returned from the crawl.log.
     * @throws NoSuchElementException If there are no more items 
     */
    public CrawlDataItem next() throws IOException{
        if(hasNext()){
            CrawlDataItem tmp = next;
            this.next = null;
            return tmp;
        }
        throw new NoSuchElementException("No more items");
    }

    /**
     * Ready the next item.  When the method returns, either next is non-null
     * or there are no more items in the crawl log.
     */
    protected void prepareNext() throws IOException{
    	if (next!=null) {
    		throw new IllegalStateException("Can't prepare next, when next is non-null");
    	}
        String line;
        while ((line = in.readLine()) != null) {
            next = parseLine(line);
            if (next != null) {
                return;
            }
        }
     }

    /** 
     * Parse a line in the crawl log.
     * <p>
     * Override this method to change how individual crawl log
     * items are processed and accepted/rejected.  This method is called from
     * within the loop in prepareNext().
     *
     * @param line A line from the crawl log.  Must not be null.
     * @return A {@link CrawlDataItem} if the next line in the crawl log yielded 
     *         a usable item, null otherwise.
     */
    protected CrawlDataItem parseLine(String line) throws IOException {
        if (line != null && line.length() > 42) {
            // Split the line up by whitespaces.
            // Limit to 12 parts (annotations may contain spaces, but will
            // always be at the end of each line.
            String[] lineParts = line.split("\\s+",12);
            
            if(lineParts.length<10){
                log.debug("Ignoring malformed line, lineParts are fewer then 10 in line:\n" + line);
                return null;
            }
            
            // Index 0: Timestamp, this is the time of log writing and not of interest
            String timestamp;
            
            // Index 1: status return code 
            int status = Integer.parseInt(lineParts[1]);
            if (status<=0) {
            	log.debug("Ignoring failed fetch based on status code. Line:\n" + line);
            	return null;
            }
            
            // Index 2: File size (ignore) 

            // Index 3: URL
            String url = lineParts[3];
            
            // Index 4: Hop path (ignore)
            // Index 5: Parent URL (ignore)
            
            // Index 6: Mime type
            String mime = lineParts[6];

            // Index 7: ToeThread number (ignore)
            
            // Index 8: ArcTimeAndDuration, this is the time when fetch begin, followed by a plus 
            //          sign and the number of milliseconds the fetch took.
            // Convert from the crawl log dateformat to w3c-iso8601
            try {
            	String fetchBegan = lineParts[8];
           		fetchBegan = fetchBegan.substring(0,fetchBegan.indexOf("+")); // Ignore + fetch duration
				timestamp = DateUtils.getLog14Date(DateUtils.parse17DigitDate(fetchBegan));
			} catch (ParseException e1) {
				throw new IOException(e1);
			}
            

            // Index 9: Digest
            String digest = lineParts[9];
            // The digest may contain a prefix. 
            // The prefix will be terminated by a : which is immediately 
            // followed by the actual digest
            if(digest.lastIndexOf(":") >= 0){
            	digest = digest.substring(digest.lastIndexOf(":")+1);
            }
            
            // Index 10: Source tag (ignore)
            
            // Index 11: Annotations (may be missing)
            boolean revisit = false;
            String originalURL = null;
            String originalTimestamp = null;
            if(lineParts.length==12){
            	if (lineParts[11].matches(revisitMatchingRegex)) {
            		revisit=true;
            	}
            }
            // Got a valid item.
            CrawlDataItem cdi = new CrawlDataItem();
            cdi.setURL(url);
            cdi.setOriginalURL(originalURL);
            cdi.setContentDigest(digest);
            cdi.setTimestamp(timestamp);
            cdi.setOriginalTimestamp(originalTimestamp);
            cdi.setMimeType(mime);
            cdi.setRevisit(revisit);
            cdi.setStatusCode(status);
            return cdi;
        } 
        return null;
    }
    
    /**
     * Closes the crawl.log file.
     */
    public void close() throws IOException{
        IOUtils.closeQuietly(in);
    }

    /*
     * (non-Javadoc)
     * @see is.hi.bok.deduplicator.CrawlDataIterator#getSourceType()
     */
    public String getSourceType() {
        return "Handles Heritrix style crawl.log files";
    }

}
