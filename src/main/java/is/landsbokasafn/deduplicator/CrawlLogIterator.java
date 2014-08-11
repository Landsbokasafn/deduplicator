/* CrawlLogIterator
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.NoSuchElementException;

/**
 * An implementation of a  {@link is.hi.bok.deduplicator.CrawlDataIterator}
 * capable of iterating over a Heritrix's style <code>crawl.log</code>.
 * 
 * @author Kristinn Sigur&eth;sson
 * @author Lars Clausen
 */
public class CrawlLogIterator implements CrawlDataIterator {

	// Date format as specified for WARC-Date and WARC-Refers-To-Date
    private SimpleDateFormat sdfWarc = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    // Date format as used in Heritrix crawl.log
    private SimpleDateFormat sdfCrawlLog = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

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
    	// Noop constructor
    }
    
    /** 
     * Create a new CrawlLogIterator that reads items from a Heritrix crawl.log
     *
     * @param source The path of a Heritrix crawl.log file.
     * @throws IOException If errors were found reading the log.
     */
    public void initialize(String source) throws IOException {
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
     * @return An item from the crawl log.  Note that unlike the Iterator
     *         interface, this method returns null if there are no more items 
     *         to fetch.
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
     * <p>
     * Note: This method should only be called when <code>next==null<code>
     */
    protected void prepareNext() throws IOException{
        String line;
        while ((line = in.readLine()) != null) {
            next = parseLine(line);
            if (next != null) {
                return;
            }
        }
     }

    /** 
     * Parse the a line in the crawl log.
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
                // If the lineParts are fewer then 10 then the line is 
                // malformed.
                return null;
            }
            
            // Index 0: Timestamp 
            String timestamp;
            // Convert from the crawl log dateformat to w3c-iso8601
            try {
				timestamp = sdfWarc.format(sdfCrawlLog.parse(lineParts[0]));
			} catch (ParseException e1) {
				throw new IOException(e1);
			}
            
            // Index 1: status return code 
            int status = Integer.parseInt(lineParts[1]);
            
            // Index 2: File size (ignore) 

            // Index 3: URL
            String url = lineParts[3];
            
            // Index 4: Hop path (ignore)
            // Index 5: Parent URL (ignore)
            
            // Index 6: Mime type
            String mime = lineParts[6];

            // Index 7: ToeThread number (ignore)
            // Index 8: ArcTimeAndDuration (ignore)

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
                // Have an annotation field. Look for original URL+Timestamp inside it.
                // Can be found in the 'annotations' field, preceded by
                // 'revisitOf:' (no quotes) and contained within a pair of 
                // double quotes. Example: revisit:"TIMESTAMP URL". 
                // May also just be a timestamp or missing altogether. Timestamp will use same 
            	// format as the timestamp at the front of the crawl log (part 0), e.g. w3c-iso8601
            	// If this information is provided, use that in lieu of 0 and 3 and DO NOT mark as revisit
            	
                String annotation = lineParts[11];
    
                int startIndex = annotation.indexOf("revisitOf:\"");
                if(startIndex >= 0){
                    // The annotation field contains revisit of info. Extract it.
                    startIndex += 10; // Skip over the 'revisitOf:"' part
                    int endIndex = annotation.indexOf('"',startIndex+1);
                    String revisitOf = annotation.substring(startIndex,endIndex);

                    
                    // The w3c-iso8601 requires exactly 24 characters
                    if (revisitOf.length()<24 || revisitOf.length()==25) {
                    	throw new IllegalStateException("revisitOf annotation field invalid: " + annotation);
                    } else if (revisitOf.length()==24 ||
                    		(revisitOf.length()==26 && revisitOf.charAt(25)=='-')) {
                    	// Just the timestamp
                    	originalTimestamp = revisitOf;
                    } else {
                    	originalTimestamp = revisitOf.substring(0,23);
                    	originalURL = revisitOf.substring(25);
                    }
                    
                    // A little sanity checking ...
                    // TODO: Shouldn't this be done in DigestIndexer?
                    try {
						sdfWarc.parse(originalTimestamp);
					} catch (ParseException e) {
						throw new IllegalStateException("Timestamp in annotation does not conform to w3c-iso8601 "
								+ "in line: " + line, e);
					}
                    
                } else if(annotation.contains("warcRevisit")){
                	// Is a duplicate of an URL from an earlier crawl but
                	// no information is available about the original capture
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
        in.close();
    }

    /*
     * (non-Javadoc)
     * @see is.hi.bok.deduplicator.CrawlDataIterator#getSourceType()
     */
    public String getSourceType() {
        return "Handles Heritrix style crawl.log files";
    }

}
