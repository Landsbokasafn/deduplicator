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
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.FSDirectory;
import org.archive.util.ArchiveUtils;

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

    // Indexing modes (by url, by hash or both)
    /** Index URL enabling lookups by URL. If normalized URLs are included
     *  in the index they will also be indexed and searchable. **/
    public static final String MODE_URL = "URL";
    /** Index HASH enabling lookups by hash (content digest) **/
    public static final String MODE_HASH = "HASH";
    /** Both URL and hash are indexed **/
    public static final String MODE_BOTH = "BOTH";

    /** The index being manipulated **/
    IndexWriter index;
    
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
     * @param indexingMode Index {@link #MODE_URL}, {@link #MODE_HASH} or 
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
        } else if(indexingMode.equals(MODE_HASH)){
            indexURL = false;
        }

        // Set up the index writer
        index = new IndexWriter(
        		FSDirectory.open(new File(indexLocation)),
                new WhitespaceAnalyzer(),
                !addToExistingIndex,
                MaxFieldLength.UNLIMITED);
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
            String mimefilter, 
            boolean blacklist,
            boolean verbose) 
            throws IOException {
        return writeToIndex(dataIt, mimefilter, blacklist, verbose, false);
    }

    /**
     * Writes the contents of a {@link CrawlDataIterator} to this index.
     * <p>
     * This method may be invoked multiple times with different 
     * CrawlDataIterators until {@link #close(boolean)} has been called.
     * 
     * @param dataIt The CrawlDataIterator that provides the data to index.
     * @param mimeFilter A regular expression that is used as a filter on the 
     *                   mimetypes to include in the index. 
     * @param blacklist If true then the <code>mimefilter</code> is used
     *                  as a blacklist for mimetypes. If false then the
     *                  <code>mimefilter</code> is treated as a whitelist. 
     * @param defaultOrigin If an item is missing an origin, this default value
     *                      will be assigned to it. Can be null if no default
     *                      origin value should be assigned.
     * @param verbose If true then progress information will be sent to 
     *                System.out.
     * @param skipDuplicates Do not add URLs that are marked as duplicates to the index
     * @return The number of items added to the index.
     * @throws IOException If an error occurs writing the index.
     */
    public long writeToIndex(
            CrawlDataIterator dataIt, 
            String mimeFilter, 
            boolean blacklist,
            boolean verbose,
            boolean skipDuplicates) 
            throws IOException {

        int count = 0;
        int skipped = 0;
        while (dataIt.hasNext()) {
            CrawlDataItem item = dataIt.next();
            if (	!(skipDuplicates && item.revisit) &&				    // Check for duplicates TODO: Look into this
            		item.getStatusCode()==200 &&                            // Only index 200s
                    item.getMimeType().matches(mimeFilter) != blacklist) {  // Apply mime-filter 
                // Ok, we wish to index this URL/Digest
                count++;
                if(verbose && count%10000==0){
                    System.out.println("Indexed " + count + " - Last URL " +
                    		"from " + item.getTimestamp());
                }
                Document doc = new Document();

                // Add URL to document.
                if (item.getURL().contains("\"")) {
                	throw new IllegalStateException("Double quotes in URLs should always be properly escaped. " 
                			+ item.getURL());
                }
                doc.add(new Field(
                        FIELD_URL,
                        item.getURL(),
                        Field.Store.YES,
                        (indexURL ? Field.Index.NOT_ANALYZED : Field.Index.NO)
                        ));
                if(equivalent){
                    doc.add(new Field(
                            FIELD_URL_NORMALIZED,
                            stripURL(item.getURL()),
                            Field.Store.YES,
                            (indexURL ? 
                                    Field.Index.NOT_ANALYZED : Field.Index.NO)
                    ));
                }

                // Add digest to document
                doc.add(new Field(
                        FIELD_DIGEST,
                        item.getContentDigest(),
                        Field.Store.YES,
                        (indexDigest ? 
                                Field.Index.NOT_ANALYZED : Field.Index.NO)
                        ));
                
                // add timestamp?
                doc.add(new Field(
                        FIELD_TIMESTAMP,
                        item.getTimestamp(),
                        Field.Store.YES,
                        Field.Index.NO
                        ));

                // Include etag?
                if(etag && item.getEtag()!=null){
                    doc.add(new Field(
                            FIELD_ETAG,
                            item.getEtag(),
                            Field.Store.YES,
                            Field.Index.NO
                            ));
                }
                index.addDocument(doc);
            } else {
                skipped++;
            }
        }
        if(verbose){
            System.out.println("Indexed " + count + " items (skipped " + skipped + ")");
        }
        return count;
    }
    
    /**
     * Close the index.
     * @param optimize If true then the index will be optimized before it is
     *                 closed.
     * @throws IOException If an error occurs optimizing or closing the index.
     */
    public void close(boolean optimize) throws IOException{
        if(optimize){
            index.optimize();
        }
        index.close();
    }

    /**
	 * An aggressive URL normalizer. This methods removes any www[0-9]. 
	 * segments from an URL, along with any trailing slashes and all
	 * parameters.
	 * <p>
	 * Example:
	 * <code>http://www.bok.hi.is/?lang=ice</code> would become
	 * <code>http://bok.hi.is</code>
	 * @param url The url to strip
	 * @return A normalized URL.
	 */
    // TODO: Use URL canonicalizer in place of this crude thing
	public static String stripURL(String url){
		String strippedUrl = url.replaceAll("www[0-9]*\\.","");
		strippedUrl = url.replaceAll("\\?.*$","");
		strippedUrl = url.replaceAll("/$","");
		return strippedUrl;
	}
    

    @SuppressWarnings({"unchecked","rawtypes"})
	public static void main(String[] args) throws Exception {
        CommandLineParser clp = 
            new CommandLineParser(args,new PrintWriter(System.out));
        long start = System.currentTimeMillis();

        // Set default values for all settings.
        boolean etag = false;
        boolean equivalent = false;
        String indexMode = MODE_BOTH;
        boolean addToIndex = false;
        String mimefilter = "^text/.*";
        boolean blacklist = true;
        String iteratorClassName = CrawlLogIterator.class.getName();
        boolean skipDuplicates = false;

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
            case 'd' : skipDuplicates = true; break;
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
        Constructor co =
            cl.getConstructor(new Class[] { String.class });
        CrawlDataIterator iterator = (CrawlDataIterator) co.newInstance(
                new Object[] { (String)cargs.get(0) });

        // Print initial stuff
        System.out.println("Indexing: " + cargs.get(0));
        System.out.println(" - Mode: " + indexMode);
        System.out.println(" - Mime filter: " + mimefilter + 
                " (" + (blacklist?"blacklist":"whitelist")+")");
        System.out.println(" - Includes" + 
                (equivalent?" <equivalent URL>":"") +
                (etag?" <etag>":""));
        System.out.println(" - Skip duplicates: " + 
                (skipDuplicates?"yes":"no"));
        System.out.println(" - Iterator: " + iteratorClassName);
        System.out.println("   - " + iterator.getSourceType());
        System.out.println("Target: " + cargs.get(1));
        if(addToIndex){
            System.out.println(" - Add to existing index (if any)");
        } else {
            System.out.println(" - New index (erases any existing index at " +
                    "that location)");
        }
        
        DigestIndexer di = new DigestIndexer((String)cargs.get(1),indexMode,
                equivalent, etag,addToIndex);
        
        // Create the index
        di.writeToIndex(iterator, mimefilter, blacklist, true, skipDuplicates);
        
        // Clean-up
        di.close(true);
        
        System.out.println("Total run time: " + 
        		ArchiveUtils.formatMillisecondsToConventional(
                        System.currentTimeMillis()-start));
    }
}
