/* DeDuplicator
 * 
 * Created on 10.04.2006
 *
 * Copyright (C) 2006-2010 National and University Library of Iceland
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
package is.landsbokasafn.deduplicator.heritrix;

import static is.landsbokasafn.deduplicator.IndexFields.DATE;
import static is.landsbokasafn.deduplicator.IndexFields.DIGEST;
import static is.landsbokasafn.deduplicator.IndexFields.ORIGINAL_RECORD_ID;
import static is.landsbokasafn.deduplicator.IndexFields.URL;
import static is.landsbokasafn.deduplicator.IndexFields.URL_CANONICALIZED;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.NIOFSDirectory;
import org.archive.modules.CrawlURI;
import org.archive.modules.ProcessResult;
import org.archive.modules.Processor;
import org.archive.modules.net.ServerCache;
import org.archive.modules.revisit.IdenticalPayloadDigestRevisit;
import org.archive.util.ArchiveUtils;
import org.archive.util.Base32;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Heritrix compatible processor.
 * <p>
 * Will determine if CrawlURIs are <i>duplicates</i>. 
 * <p>
 * Duplicate detection can only be performed <i>after</i> the fetch processors
 * have run.
 * 
 * @author Kristinn Sigur&eth;sson
 */
public class DeDuplicator extends Processor implements InitializingBean {

    private static Logger logger =
        Logger.getLogger(DeDuplicator.class.getName());

    // Spring configurable parameters
    
    /* Location of Lucene Index to use for lookups */
    private final static String ATTR_INDEX_LOCATION = "index-location";

    public String getIndexLocation() {
        return (String) kp.get(ATTR_INDEX_LOCATION);
    }
    public void setIndexLocation(String indexLocation) {
        kp.put(ATTR_INDEX_LOCATION,indexLocation);
    }

    /* The matching method in use (by url or content digest) */
    private final static String ATTR_MATCHING_METHOD = "matching-method";
    private final static MatchingMethod DEFAULT_MATCHING_METHOD = MatchingMethod.URL; 
    {
        setMatchingMethod(DEFAULT_MATCHING_METHOD);
    }
    public MatchingMethod getMatchingMethod() {
        return (MatchingMethod) kp.get(ATTR_MATCHING_METHOD);
    }
    public void setMatchingMethod(MatchingMethod matchinMethod) {
    		kp.put(ATTR_MATCHING_METHOD, matchinMethod);
    }
    
    /* If an exact match is not made, should the processor try 
     *  to find an equivalent match?  
     */
    public final static String ATTR_CANONICAL = "try-canonical";
    {
    	setTryCanonical(false);
    }
    public boolean getTryCanonical(){
    	return (Boolean)kp.get(ATTR_CANONICAL);
    }
    public void setTryCanonical(boolean tryEquivalent){
    	kp.put(ATTR_CANONICAL,tryEquivalent);
    }

    /* The filter on mime types. This is either a blacklist or whitelist
     *  depending on ATTR_FILTER_MODE.
     */
    public final static String ATTR_MIME_FILTER = "mime-filter";
    public final static String DEFAULT_MIME_FILTER = "^text/.*";
    {
    	setMimeFilter(DEFAULT_MIME_FILTER);
    }
    public String getMimeFilter(){
    	return (String)kp.get(ATTR_MIME_FILTER);
    }
    public void setMimeFilter(String mimeFilter){
    	kp.put(ATTR_MIME_FILTER, mimeFilter);
    }

    /* Is the mime filter a blacklist (do not apply processor to what matches) 
     *  or whitelist (apply processor only to what matches).
     */
    public final static String ATTR_FILTER_MODE = "filter-mode";
    {
    	setBlacklist(true);
    }
    public boolean getBlacklist(){
    	return (Boolean)kp.get(ATTR_FILTER_MODE);
    }
    public void setBlacklist(boolean blacklist){
    	kp.put(ATTR_FILTER_MODE, blacklist);
    }
    
    /* Should statistics be tracked per host? **/
    public final static String ATTR_STATS_PER_HOST = "stats-per-host";
    {
    	setStatsPerHost(false);
    }
    public boolean getStatsPerHost(){
    	return (Boolean)kp.get(ATTR_STATS_PER_HOST);
    }
    public void setStatsPerHost(boolean statsPerHost){
    	kp.put(ATTR_STATS_PER_HOST,statsPerHost);
    }

    // Spring configured access to Heritrix resources
    
    // Gain access to the ServerCache for host based statistics.
    protected ServerCache serverCache;
    public ServerCache getServerCache() {
        return this.serverCache;
    }
    @Autowired
    public void setServerCache(ServerCache serverCache) {
        this.serverCache = serverCache;
    }
    
    // TODO: Consider making configurable. Needs to match what is written to the index though.
    AggressiveUrlCanonicalizer canonicalizer = new AggressiveUrlCanonicalizer();

    
    // Member variables.
    
    protected IndexSearcher searcher = null;
    protected DirectoryReader dReader = null;
    protected boolean lookupByURL = true;
    protected boolean statsPerHost = false;
    
    protected Statistics stats = null;
    protected HashMap<String, Statistics> perHostStats = null;

    public void afterPropertiesSet() throws Exception {
        // Index location
        String indexLocation = getIndexLocation();
        try {
            dReader = DirectoryReader.open(new NIOFSDirectory(new File(indexLocation)));
            searcher = new IndexSearcher(dReader);
        } catch (Exception e) {
        	throw new IllegalArgumentException("Unable to find/open index at " + indexLocation,e);
        } 
        
        // Matching method
        MatchingMethod matchingMethod = getMatchingMethod();
        lookupByURL = matchingMethod == MatchingMethod.URL;

        // Track per host stats
        statsPerHost = getStatsPerHost();
        
        // Initialize some internal variables:
        stats = new Statistics();
        if (statsPerHost) {
            perHostStats = new HashMap<String, Statistics>();
        }
    }
    

	@Override
	protected boolean shouldProcess(CrawlURI curi) {
        if (curi.is2XXSuccess() == false) {
            // Early return. No point in doing comparison on failed downloads.
            logger.finest("Not handling " + curi.toString()
                    + ", did not succeed.");
            return false;
        }
        if (curi.isPrerequisite()) {
            // Early return. Prerequisites are exempt from checking.
            logger.finest("Not handling " + curi.toString()
                    + ", prerequisite.");
            return false;
        }
        if (curi.toString().startsWith("http")==false) {
            // Early return. Non-http documents are not handled at present
            logger.finest("Not handling " + curi.toString()
                        + ", non-http.");
            return false;
        }
        if(curi.getContentType() == null){
            // No content type means we can not handle it.
            logger.finest("Not handling " + curi.toString()
                    + ", missing content (mime) type");
            return false;
        }
        if(curi.getContentType().matches(getMimeFilter()) == getBlacklist()){
            // Early return. Does not pass the mime filter
            logger.finest("Not handling " + curi.toString()
                    + ", excluded by mimefilter (" + 
                    curi.getContentType() + ").");
            return false;
        }
        if(curi.isRevisit()){
            // Early return. A previous processor or filter has judged this
            // CrawlURI to be a revisit
            logger.finest("Not handling " + curi.toString()
                    + ", already flagged as revisit.");
            return false;
        }
        return true;
	}

    @Override
    protected void innerProcess(CrawlURI puri) {
    	throw new AssertionError();
    }

	
	@Override
	protected ProcessResult innerProcessResult(CrawlURI curi) throws InterruptedException {
        logger.finest("Processing " + curi.toString() + "(" + curi.getContentType() + ")");

        stats.handledNumber++;
        stats.totalAmount += curi.getContentSize();
        Statistics currHostStats = null;
        if(statsPerHost){
            synchronized (perHostStats) {
                String host = getServerCache().getHostFor(curi.getUURI()).getHostName();
                currHostStats = perHostStats.get(host);
                if(currHostStats==null){
                    currHostStats = new Statistics();
                    perHostStats.put(host,currHostStats);
                }
            }
            currHostStats.handledNumber++;
            currHostStats.totalAmount += curi.getContentSize();
        }
        
        Document duplicate = null; 
        
        if(lookupByURL){
            duplicate = lookupByURL(curi,currHostStats);
        } else {
            duplicate = lookupByDigest(curi,currHostStats);
        }

        if (duplicate != null){
            // Perform tasks common to when a duplicate is found.
            // Increment statistics counters
            stats.duplicateAmount += curi.getContentSize();
            stats.duplicateNumber++;
            if(statsPerHost){ 
                currHostStats.duplicateAmount+=curi.getContentSize();
                currHostStats.duplicateNumber++;
            }

            // Attach revisit profile to CURI. This will inform downstream processors that we've 
            // marked this as a duplicate/revisit
            String duplicateTimestamp = duplicate.get(DATE.name());
            String duplicateURL = duplicate.get(URL.name());
            
        	IdenticalPayloadDigestRevisit revisitProfile = 
        			new IdenticalPayloadDigestRevisit(curi.getContentDigestString());
        	
        	revisitProfile.setRefersToTargetURI(duplicateURL);
        	String refersToDate = duplicateTimestamp;
       		revisitProfile.setRefersToDate(refersToDate);
        	
        	String refersToRecordID = duplicate.get(ORIGINAL_RECORD_ID.name());
        	if (refersToRecordID!=null && !refersToRecordID.isEmpty()) {
        		revisitProfile.setRefersToRecordID(refersToRecordID);
        	}
        	
        	curi.setRevisitProfile(revisitProfile);

        	// Add annotation to crawl.log 
            String annotation = "Revisit:IdenticalPayloadDigest";
            curi.getAnnotations().add(annotation);
        }
        
        return ProcessResult.PROCEED;
	}

	/** 
     * Process a CrawlURI looking up in the index by URL
     * @param curi The CrawlURI to process
     * @param currHostStats A statistics object for the current host.
     *                      If per host statistics tracking is enabled this
     *                      must be non null and the method will increment
     *                      appropriate counters on it.
     * @return The result of the lookup (a Lucene document). If a duplicate is
     *         not found null is returned.
     */
	protected Document lookupByURL(CrawlURI curi, Statistics currHostStats) {
		// Look the CrawlURI's URL up in the index.
		try {
			Query query = queryField(URL.name(), curi.getURI());
			ScoreDoc[] hits = searcher.search(query, null, 5).scoreDocs;

			Document doc = null;
			String currentDigest = curi.getContentDigestString();
			if (hits != null && hits.length > 0) {
				// Typically there should only be one hit, but we'll allow for
				// multiple hits.
				for (int i = 0; i < hits.length; i++) {
					// Multiple hits on same exact URL should be rare
					// See if any have matching content digests
					doc = searcher.doc(hits[i].doc);
					String oldDigest = doc.get(DIGEST.name());

					if (oldDigest.equalsIgnoreCase(currentDigest)) {
						stats.exactURLDuplicates++;
						if (statsPerHost) {
							currHostStats.exactURLDuplicates++;
						}

						logger.finest("Found exact match for "
								+ curi.toString());

						// If we found a hit, no need to look at other hits.
						return doc;
					}
				}
			}
			if (getTryCanonical()) {
				// No exact hits. Let's try lenient matching.
				String canonicalizedURL = canonicalizer.canonicalize(curi.toString()); 
				query = queryField(URL.name(), canonicalizedURL);
				hits = searcher.search(query, null, 5).scoreDocs;

				for (int i = 0; i < hits.length; i++) {
					doc = searcher.doc(hits[i].doc);
					String indexDigest = doc.get(DIGEST.name());
					if (indexDigest.equals(currentDigest)) {
						// Make note in log
						String equivURL = doc.get(URL.name());
						curi.getAnnotations().add(
								"equivalentURL:\"" + equivURL + "\"");
						// Increment statistics counters
						stats.canonicalURLDuplicates++;
						if (statsPerHost) {
							currHostStats.canonicalURLDuplicates++;
						}
						logger.finest("Found equivalent match for "
								+ curi.toString() + ". Canonicalized: "
								+ canonicalizedURL + ". Equivalent to: "
								+ equivURL);

						// If we found a hit, no need to look at more.
						return doc;
					}
				}
			}
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Error accessing index.", e);
		}
		// If we make it here then this is not a duplicate.
		return null;
	}
    
	/** 
     * Process a CrawlURI looking up in the index by content digest
     * @param curi The CrawlURI to process
     * @param currHostStats A statistics object for the current host.
     *                      If per host statistics tracking is enabled this
     *                      must be non null and the method will increment
     *                      appropriate counters on it.
     * @return The result of the lookup (a Lucene document). If a duplicate is
     *         not found null is returned.
     */
    protected Document lookupByDigest(CrawlURI curi, Statistics currHostStats) {
        Document duplicate = null; 
        String currentDigest = null;
        Object digest = curi.getContentDigest();
        if (digest != null) {
            currentDigest = Base32.encode((byte[])digest);
        }
        Query query = queryField(DIGEST.name(), currentDigest);
        try {
            ScoreDoc[] hits = searcher.search(query, null, 50).scoreDocs; // TODO: Look at value 50
            StringBuffer mirrors = new StringBuffer();
            mirrors.append("mirrors: ");
            String url = curi.toString();
            String normalizedURL = 
            	getTryCanonical() ? canonicalizer.canonicalize(url) : null;
            if(hits != null && hits.length > 0){
                // Can definitely be more then one
                // Note: We may find an equivalent match before we find an
                //       (existing) exact match. 
                // TODO: Ensure that an exact match is recorded if it exists.
                for(int i=0 ; i<hits.length && duplicate==null ; i++){
                    Document doc = searcher.doc(hits[i].doc);
                    String indexURL = doc.get(URL.name());
                    // See if the current hit is an exact match.
                    if(url.equals(indexURL)){
                        duplicate = doc;
                        stats.exactURLDuplicates++;
                        if(statsPerHost){
                            currHostStats.exactURLDuplicates++;
                        }
                        logger.finest("Found exact match for " + curi.toString());
                    }
                    
                    // If not, then check if it is an equivalent match (if
                    // equivalent matches are allowed).
                    if(duplicate == null && getTryCanonical()){
                        String indexNormalizedURL = 
                            doc.get(URL_CANONICALIZED.name());
                        if(normalizedURL.equals(indexNormalizedURL)){
                            duplicate = doc;
                            stats.canonicalURLDuplicates++;
                            if(statsPerHost){
                                currHostStats.canonicalURLDuplicates++;
                            }
                            curi.getAnnotations().add("equivalentURL:\"" + indexURL + "\"");
                            logger.finest("Found equivalent match for " + 
                                    curi.toString() + ". Normalized: " + 
                                    normalizedURL + ". Equivalent to: " + indexURL);
                        }
                    }
                    
                    if(duplicate == null){
                        // Will only be used if no exact (or equivalent) match
                        // is found.
                        mirrors.append(indexURL + " ");
                    }
                }
                if(duplicate == null){
                    stats.mirrorNumber++;
                    if (statsPerHost) {
                        currHostStats.mirrorNumber++;
                    }
                    logger.log(Level.FINEST,"Found mirror URLs for " + 
                            curi.toString() + ". " + mirrors);
                }
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE,"Error accessing index.",e);
        }
        return duplicate;
    }
    
	public String report() {
        StringBuffer ret = new StringBuffer();
        ret.append("Processor: is.hi.bok.digest.DeDuplicator\n");
        ret.append("  Function:          Abort processing of duplicate records\n");
        ret.append("                     - Lookup by " + 
        		(lookupByURL?"url":"digest") + " in use\n");
        ret.append("  Total handled:     " + stats.handledNumber + "\n");
        ret.append("  Duplicates found:  " + stats.duplicateNumber + " " + 
        		getPercentage(stats.duplicateNumber,stats.handledNumber) + "\n");
        ret.append("  Bytes total:       " + stats.totalAmount + " (" + 
        		ArchiveUtils.formatBytesForDisplay(stats.totalAmount) + ")\n");
        ret.append("  Bytes discarded:   " + stats.duplicateAmount + " (" + 
        		ArchiveUtils.formatBytesForDisplay(stats.duplicateAmount) + ") " + 
        		getPercentage(stats.duplicateAmount, stats.totalAmount) + "\n");
        
    	ret.append("  New (no hits):     " + (stats.handledNumber-
    			(stats.mirrorNumber+stats.exactURLDuplicates+stats.canonicalURLDuplicates)) + "\n");
    	ret.append("  Exact hits:        " + stats.exactURLDuplicates + "\n");
    	ret.append("  Canonical hits:    " + stats.canonicalURLDuplicates + "\n");
        if(lookupByURL==false){
        	ret.append("  Mirror hits:       " + stats.mirrorNumber + "\n");
        }
        
        if(statsPerHost){
            ret.append("  [Host] [total] [duplicates] [bytes] " +
                    "[bytes discarded] [new] [exact] [equiv]");
            if(lookupByURL==false){
                ret.append(" [mirror]");
            }
            ret.append("\n");
            synchronized (perHostStats) {
                Iterator<String> it = perHostStats.keySet().iterator();
                while(it.hasNext()){
                    String key = it.next();
                    Statistics curr = perHostStats.get(key);
                    ret.append("  " +key);
                    ret.append(" ");
                    ret.append(curr.handledNumber);
                    ret.append(" ");
                    ret.append(curr.duplicateNumber);
                    ret.append(" ");
                    ret.append(curr.totalAmount);
                    ret.append(" ");
                    ret.append(curr.duplicateAmount);
                    ret.append(" ");
                    ret.append(curr.handledNumber-
                            (curr.mirrorNumber+
                             curr.exactURLDuplicates+
                             curr.canonicalURLDuplicates));
                    ret.append(" ");
                    ret.append(curr.exactURLDuplicates);
                    ret.append(" ");
                    ret.append(curr.canonicalURLDuplicates);

                    if(lookupByURL==false){
                        ret.append(" ");
                        ret.append(curr.mirrorNumber);
                    }    
                    ret.append("\n");
                }
            }
        }
        
        ret.append("\n");
        return ret.toString();
	}
	
	protected static String getPercentage(double portion, double total){
		double value = portion / total;
		value = value*100;
		String ret = Double.toString(value);
		int dot = ret.indexOf('.');
		if(dot+3<ret.length()){
			ret = ret.substring(0,dot+3);
		}
		return ret + "%";
	}

    /** Run a simple Lucene query for a single term in a single field.
     *
     * @param fieldName name of the field to look in.
     * @param value The value to query for
     * @returns A Query for the given value in the given field.
     */
    protected Query queryField(String fieldName, String value) {
    	Query query = new TermQuery(new Term(fieldName,value));
        return query;
    }

	
	protected void finalTasks() {
		try {
			if (dReader != null) {
				dReader.close();
			}
		} catch (IOException e) {
			logger.log(Level.SEVERE,"Error closing index",e);
		}
	}


}

class Statistics{
    // General statistics
    
    /** Number of URIs that make it through the processors exclusion rules
     *  and are processed by it.
     */
    long handledNumber = 0;
    
    /** Number of URIs that are deemed duplicates and further processing is
     *  aborted
     */
    long duplicateNumber = 0;
    
    /** Then number of URIs that turned out to have exact URL and content 
     *  digest matches.
     */
    long exactURLDuplicates = 0;
    
    /** The number of URIs that turned out to have equivalent URL and content
     *  digest matches.
     */
    long canonicalURLDuplicates = 0;
    
    /** The number of URIs that, while having no exact or equivalent matches,  
     *  do have exact content digest matches against non-equivalent URIs.
     */
    long mirrorNumber = 0;
    
    /** The total amount of data represented by the documents who were deemed
     *  duplicates and excluded from further processing.
     */
    long duplicateAmount = 0;
    
    /** The total amount of data represented by all the documents processed **/
    long totalAmount = 0;

}

