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
import java.util.NoSuchElementException;

/**
 * An implementation of a  {@link is.hi.bok.deduplicator.CrawlDataIterator}
 * capable of iterating over a Heritrix's style <code>crawl.log</code>.
 * 
 * @author Kristinn Sigur&eth;sson
 * @author Lars Clausen
 */
public class CrawlLogIterator extends CrawlDataIterator {

    /** 
     * A reader for the crawl.log file being processed
     */
    protected BufferedReader in;
    
    /**
     * The next item to be issued (if ready) or null if the next item
     * has not been prepared or there are no more elements 
     */
    protected CrawlDataItem next;
    
    /** 
     * Create a new CrawlLogIterator that reads items from a Heritrix crawl.log
     *
     * @param source The path of a Heritrix crawl.log file.
     * @throws IOException If errors were found reading the log.
     */
    public CrawlLogIterator(String source) 
            throws IOException {
        super(source);
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
     * Ready the next item.  This method will skip over items that
     * getNextItem() rejects.  When the method returns, either next is non-null
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
    protected CrawlDataItem parseLine(String line) {
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
            // Crawl logs use the same date format as specified by the WARC standard for WARC-Date, w3c-iso8601
            timestamp = lineParts[0];
            
            // Index 1: status return code 
            int status = Integer.parseInt(lineParts[1]);
            
            // Index 2: File size 
            long size = -1;
            if (lineParts[2].equals("-")) {
            	// If size is missing then this URL was not successfully visited. Skip in index
            	return null;
            }
            try {
            	size = Long.parseLong(lineParts[2]);
            } catch (NumberFormatException e) {
                System.err.println("Error parsing size for: " + line + 
                		" Item: " + lineParts[2] + " Message: " + e.getMessage());
            }

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
            if(lineParts.length==12){
                // Have an annotation field. Look for original URL+Timestamp inside it.
                // Can be found in the 'annotations' field, preceded by
                // 'revisitOf:' (no quotes) and contained within a pair of 
                // double quotes. Example: revisit:"TIMESTAMP URL". 
                // May also just be a timestamp or missing altogether. Timestamp will use same 
            	// format as the timestamp at the front of the crawl log (part 0)
            	// If this information is provided, use that in lieu of 0 and 3 and DO NOT mark as revisit
            	
            	// TODO: FIGURE OUT THIS PART!!!!!
                String annotation = lineParts[11];
    
                int startIndex = annotation.indexOf("revisitOf:\"");
                if(startIndex >= 0){
                    // The annotation field contains origin info. Extract it.
                    startIndex += 10; // Skip over the 'revisitOf:"' part
                    int endIndex = annotation.indexOf('"',startIndex+1);
                    // That also means this is a duplicate of an URL from an 
                    // earlier crawl
                    revisit=true;
                } else if(annotation.contains("warcRevisit")){
                	// Is a duplicate of an URL from an earlier crawl but
                	// no origin information was recorded
                	revisit=true;
                }
            }
            // Got a valid item.
            return new CrawlDataItem(url, digest, timestamp, null, mime, revisit, size, status, null);
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
