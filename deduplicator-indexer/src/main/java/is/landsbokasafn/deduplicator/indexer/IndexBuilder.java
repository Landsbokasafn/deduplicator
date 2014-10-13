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

import static is.landsbokasafn.deduplicator.IndexFields.DATE;
import static is.landsbokasafn.deduplicator.IndexFields.DIGEST;
import static is.landsbokasafn.deduplicator.IndexFields.ETAG;
import static is.landsbokasafn.deduplicator.IndexFields.URL;
import static is.landsbokasafn.deduplicator.IndexFields.URL_CANONICALIZED;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;

/**
 * A class for building a de-duplication index.
 *
 * @author Kristinn Sigur&eth;sson
 * 
 */
public class IndexBuilder {
	
	public static final Version LUCENE_VER = Version.LUCENE_47;
	
	public static final String WARC_DATE_FORMAT="yyyy-MM-dd'T'HH:mm:ss'Z'";
	
    /** The index being manipulated **/
    IndexWriter index;
    
    private static final AggressiveUrlCanonicalizer canonicalizer = new AggressiveUrlCanonicalizer();
    
    // The options with default settings
    boolean includeEtag = false;
    boolean includeCanonicalizedURL = false;
    boolean indexURL = true;

    /**
     * Each instance of this class wraps one Lucene index for writing 
     * deduplication information to it.
     * 
     * @param indexLocation The location of the index (path).
     * @param indexURL Index the URL field in the index.
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
            boolean indexURL,
            boolean includeCanonicalizedURL,
            boolean includeEtag,
            boolean addToExistingIndex) throws IOException {
        
    	this.indexURL = indexURL;
        this.includeEtag = includeEtag;
        this.includeCanonicalizedURL = includeCanonicalizedURL;
        
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

        // Define field types for indexed and non indexed fields. No fields are tokenized
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
            if(includeCanonicalizedURL){
                doc.add(new Field(
                        URL_CANONICALIZED.name(),
                        canonicalizer.canonicalize(item.getURL()),
                        (indexURL ? ftIndexed : ftNotIndexed)));
            }

            // Add digest to document
            doc.add(new Field(
                    DIGEST.name(),
                    item.getContentDigest(),
                    ftIndexed));
            
            // add timestamp
            doc.add(new Field(
                    DATE.name(),
                    timestamp,
                    ftNotIndexed));

            // Include etag?
            if(includeEtag && item.getEtag()!=null){
                doc.add(new Field(
                        ETAG.name(),
                        item.getEtag(),
                        ftNotIndexed));
            }
            if (indexURL) {
            	// Delete any URL+Digest matches from index first
            	BooleanQuery q = new BooleanQuery();
            	q.add(new TermQuery(new Term(URL.name(), url)), Occur.MUST);
            	q.add(new TermQuery(new Term(DIGEST.name(), item.getContentDigest())), Occur.MUST);
            	index.deleteDocuments(q);
            	index.addDocument(doc);
            } else {
                index.updateDocument(new Term(DIGEST.name()), doc);
            }
            
        }
        System.out.println("Indexed " + count + " items (unresolved " + unresolved + ", skipped " + skipped + ")");
        return count;
    }
    
    /**
     * Close the index.
     * @throws IOException If an error occurs closing the index.
     */
    public void close() throws IOException{
        index.close();
    }

}
