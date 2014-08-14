/* DigestIndexer
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
package is.landsbokasafn.deduplicator.indexer;

import static is.landsbokasafn.deduplicator.IndexFields.*;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;

/**
 * A class for building a de-duplication index.
 * <p>
 * This class also defines string constants for the lucene field names.
 *
 * @author Kristinn Sigur&eth;sson
 * 
 */
public class IndexBuilder {
	
	public static final Version LUCENE_VER = Version.LUCENE_47;
	
	public static final String WARC_DATE_FORMAT="yyyy-MM-dd'T'HH:mm:ss'Z'";
	
    // Indexing modes (by url, by digest or both)
    /** Index URL enabling lookups by URL. If normalized URLs are included
     *  in the index they will also be indexed and searchable. **/
    public static final String MODE_URL = "URL";
    /** Index digest enabling lookups by payload digest **/
    public static final String MODE_DIGEST = "DIGEST";
    /** Both URL and digest are indexed **/
    public static final String MODE_BOTH = "BOTH";

    /** The index being manipulated **/
    IndexWriter index;
    
    private static final AggressiveUrlCanonicalizer canonicalizer = new AggressiveUrlCanonicalizer();
    
    // The options with default settings
    boolean etag = false;
    boolean equivalent = false;
    boolean indexURL = true;
    boolean indexDigest = true;

    /**
     * Each instance of this class wraps one Lucene index for writing 
     * deduplication information to it.
     * 
     * @param indexLocation The location of the index (path).
     * @param indexingMode Index {@link #MODE_URL}, {@link #MODE_DIGEST} or 
     *                     {@link #MODE_BOTH}.
     * @param includeCanonicalizedURL Should a normalized version of the URL be 
     *                             added to the index. 
     *                             See {@link #stripURL(String)}.
     * @param includeTimestamp Should a timestamp be included in the index.
     * @param includeEtag Should an Etag be included in the index.
     * @param addToExistingIndex Are we opening up an existing index. Setting
     *                           this to false will cause any index at 
     *                           <code>indexLocation</code> to be overwritten.
     * @throws IOException If an error occurs opening the index.
     */
    public IndexBuilder(
            String indexLocation,
            String indexingMode,
            boolean includeCanonicalizedURL,
            boolean includeEtag,
            boolean addToExistingIndex) throws IOException {
        
        this.etag = includeEtag;
        this.equivalent = includeCanonicalizedURL;
        
        if(indexingMode.equals(MODE_URL)){
            indexDigest = false;
        } else if(indexingMode.equals(MODE_DIGEST)){
            indexURL = false;
        }

        IndexWriterConfig indexWriterConfig = 
        		new IndexWriterConfig(LUCENE_VER, new WhitespaceAnalyzer(LUCENE_VER));
        if (addToExistingIndex) {
        	indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        } else {
        	indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        }
        
        // Set up the index writer
        index = new IndexWriter(FSDirectory.open(new File(indexLocation)),indexWriterConfig); 
        
    }

    /**
     * Writes the contents of a {@link CrawlDataIterator} to this index.
     * <p>
     * This method may be invoked multiple times with different 
     * CrawlDataIterators until {@link #close(boolean)} has been called.
     * 
     * @param dataIt The CrawlDataIterator that provides the data to index.
     * @param mimefilter A regular expression that is used as a filter on the 
     *                   mimetypes to include in the index. 
     * @param blacklist If true then the <code>mimefilter</code> is used
     *                  as a blacklist for mimetypes. If false then the
     *                  <code>mimefilter</code> is treated as a whitelist. 
     * @param defaultOrigin If an item is missing an origin, this default value
     *                      will be assigned to it. Can be null if no default
     *                      origin value should be assigned.
     * @param verbose If true then progress information will be sent to 
     *                System.out.
     * @return The number of items added to the index.
     * @throws IOException If an error occurs writing the index.
     */
    public long writeToIndex(
            CrawlDataIterator dataIt, 
            String mimeFilter, 
            boolean blacklist,
            boolean verbose) 
            throws IOException {

        int count = 0;
        int skipped = 0;
        int unresolved = 0;

        // Define field types for indexed and non indexed fields. No fields are tokanized
        FieldType ftIndexed = new FieldType();
        ftIndexed.setIndexed(true);
        ftIndexed.setTokenized(false);
        ftIndexed.setStored(true);

        FieldType ftNotIndexed = new FieldType(ftIndexed);
        ftNotIndexed.setIndexed(false);
        
        while (dataIt.hasNext()) {
            CrawlDataItem item = dataIt.next();

            if (item.getStatusCode()!=200) {
            	// Only index items that were crawled without issues
            	// TODO: Consider widening to 4XXs at least
                skipped++;
            	continue;
            }
            
            if (item.getMimeType().matches(mimeFilter) == blacklist) {
            	skipped++;
            	continue;
            }

            String url = item.getURL();
            String timestamp = item.getTimestamp();

            if (item.isRevisit()) {
            	if (item.getOriginalURL()==null || item.getOriginalTimestamp()==null) {
            		// Can't index without information about the original capture 
        			unresolved++;
            		continue;
            	} else {
            		url = item.getOriginalURL();
            		timestamp = item.getOriginalTimestamp();
            	}
            }

            // Ok, we wish to index this URL/Digest
            count++;
            if(verbose && count%10000==0){
                System.out.println("Indexed " + count + ", unresolved " + unresolved + " - Last URL " +
                		"from " + item.getTimestamp());
            }

            if (url.contains("\"")) {
            	// TODO: Consider other sanity checks and also option to just log and continue on failed
            	//       sanity checks.
            	throw new IllegalStateException("Double quotes in URLs should always be properly escaped. " 
            			+ item.getURL());
            }

            // Add URL to document.
            Document doc = new Document();

            doc.add(new Field(
                    URL.name(),
                    url,
                    (indexURL ? ftIndexed : ftNotIndexed)));
            if(equivalent){
                doc.add(new Field(
                        URL_CANONICALIZED.name(),
                        canonicalizer.canonicalize(item.getURL()),
                        (indexURL ? ftIndexed : ftNotIndexed)));
            }

            // Add digest to document
            doc.add(new Field(
                    DIGEST.name(),
                    item.getContentDigest(),
                    (indexDigest ? ftIndexed : ftNotIndexed)));
            
            // add timestamp
            doc.add(new Field(
                    DATE.name(),
                    timestamp,
                    ftNotIndexed));

            // Include etag?
            if(etag && item.getEtag()!=null){
                doc.add(new Field(
                        ETAG.name(),
                        item.getEtag(),
                        ftNotIndexed));
            }
            index.updateDocument(new Term(URL.name()), doc);
        }
        if(verbose){
            System.out.println("Indexed " + count + " items (skipped " + skipped + ")");
        }
        return count;
    }
    
    /**
     * Close the index.
     * @throws IOException If an error occurs optimizing or closing the index.
     */
    public void close() throws IOException{
        index.close();
    }

}
