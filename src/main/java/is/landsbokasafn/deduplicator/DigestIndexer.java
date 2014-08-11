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
package is.landsbokasafn.deduplicator;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.archive.util.DateUtils;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;

/**
 * A class for building a de-duplication index.
 * <p>
 * The indexing can be done via the command line options (Run with --help 
 * parameter to print usage information) or natively embedded in other 
 * applications. 
 * <p>
 * This class also defines string constants for the lucene field names.
 *
 * @author Kristinn Sigur&eth;sson
 * 
 */
public class DigestIndexer {
	
	public static final Version LUCENE_VER = Version.LUCENE_47;
	
    // Lucene index field names
    /** The URL 
     *  This value is suitable for use in warc/revist records as the WARC-Refers-To-Target-URI
     **/
	public static final String FIELD_URL = "url";
    /** The content digest as String **/
	public static final String FIELD_DIGEST = "digest";
    /** The URLs timestamp (time of fetch). Suitable for use in WARC-Refers-To-Date. Encoded according to
     *  w3c-iso8601  
     */
    public static final String FIELD_TIMESTAMP = "date";
    /** The document's etag **/
    public static final String FIELD_ETAG = "etag";
    /** A stripped (canonicalized) version of the URL **/
	public static final String FIELD_URL_NORMALIZED = "url-normalized";
    /** WARC Record ID of original payload capture. Suitable for WARC-Refers-To field. **/
    public static final String FIELD_ORIGINAL_RECORD_ID="warc-record-id";

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
     * @param includeNormalizedURL Should a normalized version of the URL be 
     *                             added to the index. 
     *                             See {@link #stripURL(String)}.
     * @param includeTimestamp Should a timestamp be included in the index.
     * @param includeEtag Should an Etag be included in the index.
     * @param addToExistingIndex Are we opening up an existing index. Setting
     *                           this to false will cause any index at 
     *                           <code>indexLocation</code> to be overwritten.
     * @throws IOException If an error occurs opening the index.
     */
    public DigestIndexer(
            String indexLocation,
            String indexingMode,
            boolean includeNormalizedURL,
            boolean includeEtag,
            boolean addToExistingIndex) throws IOException{
        
        this.etag = includeEtag;
        this.equivalent = includeNormalizedURL;
        
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
            		// Can't index without those.
            		// TODO: Add lookup option?
                    skipped++;
            		continue;
            	} else {
            		url = item.getOriginalURL();
            		timestamp = item.getOriginalTimestamp();
            	}
            }

            // Ok, we wish to index this URL/Digest
            count++;
            if(verbose && count%10000==0){
                System.out.println("Indexed " + count + " - Last URL " +
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
                    FIELD_URL,
                    url,
                    (indexURL ? ftIndexed : ftNotIndexed)));
            if(equivalent){
                doc.add(new Field(
                        FIELD_URL_NORMALIZED,
                        canonicalizer.canonicalize(item.getURL()),
                        (indexURL ? ftIndexed : ftNotIndexed)));
            }

            // Add digest to document
            doc.add(new Field(
                    FIELD_DIGEST,
                    item.getContentDigest(),
                    (indexDigest ? ftIndexed : ftNotIndexed)));
            
            // add timestamp
            doc.add(new Field(
                    FIELD_TIMESTAMP,
                    timestamp,
                    ftNotIndexed));

            // Include etag?
            if(etag && item.getEtag()!=null){
                doc.add(new Field(
                        FIELD_ETAG,
                        item.getEtag(),
                        ftNotIndexed));
            }
            index.updateDocument(new Term(FIELD_URL), doc);
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

    @SuppressWarnings({"unchecked","rawtypes"})
	public static void main(String[] args) throws Exception {
		// Parse command line options    	
        CommandLineParser clp = new CommandLineParser(args,new PrintWriter(System.out));
        
        long start = System.currentTimeMillis();

        // Set default values for all settings.
        boolean etag = false;
        boolean equivalent = false;
        String indexMode = MODE_BOTH;
        boolean addToIndex = false;
        String mimefilter = "^text/.*";
        boolean blacklist = true;
        String iteratorClassName = WarcIterator.class.getName();

        // Process the options
        Option[] opts = clp.getCommandLineOptions();
        for(int i=0 ; i<opts.length ; i++){
            Option opt = opts[i];
            switch(opt.getId()){
            case 'w' : blacklist=false; break;
            case 'a' : addToIndex=true; break;
            case 'e' : etag=true; break;
            case 'h' : clp.usage(0); break;
            case 'i' : iteratorClassName = opt.getValue(); break;
            case 'm' : mimefilter = opt.getValue(); break;
            case 'o' : indexMode = opt.getValue(); break;
            case 's' : equivalent = true; break;
            }
        }
        
        List cargs = clp.getCommandLineArguments(); 
        
        if(cargs.size() != 2){
            // Should be exactly two arguments. Source and target!
            clp.usage(0);
        }

        // Get the CrawlDataIterator
        // Get the iterator classname or load default.
        Class cl = Class.forName(iteratorClassName);
        Constructor co = cl.getConstructor(new Class[] { String.class });
        CrawlDataIterator iterator = (CrawlDataIterator) co.newInstance(
                new Object[] { (String)cargs.get(0) });

        // Print initial stuff
        System.out.println("Indexing: " + cargs.get(0));
        System.out.println(" - Mode: " + indexMode);
        System.out.println(" - Mime filter: " + mimefilter + 
                " (" + (blacklist?"blacklist":"whitelist")+")");
        System.out.println(" - Includes" + 
                (equivalent?" <canonical URL>":"") +
                (etag?" <etag>":""));
        System.out.println(" - Iterator: " + iteratorClassName);
        System.out.println("   - " + iterator.getSourceType());
        System.out.println("Target: " + cargs.get(1));
        if(addToIndex){
            System.out.println(" - Add to existing index (if any)");
        } else {
            System.out.println(" - New index (erases any existing index at " +
                    "that location)");
        }

        // Create the index
        DigestIndexer di = new DigestIndexer((String)cargs.get(1),indexMode,
                equivalent, etag,addToIndex);
        di.writeToIndex(iterator, mimefilter, blacklist, true);
        
        // Clean-up
        di.close();
        
        System.out.println("Total run time: " + 
        		DateUtils.formatMillisecondsToConventional(System.currentTimeMillis()-start));
    }
}
