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
package is.landsbokasafn.deduplicator.heritrix;

import static is.landsbokasafn.deduplicator.DeDuplicatorConstants.EXTRA_REVISIT_DATE;
import static is.landsbokasafn.deduplicator.DeDuplicatorConstants.EXTRA_REVISIT_PROFILE;
import static is.landsbokasafn.deduplicator.DeDuplicatorConstants.EXTRA_REVISIT_URI;
import static is.landsbokasafn.deduplicator.DeDuplicatorConstants.REVISIT_ANNOTATION_MARKER;

import java.text.NumberFormat;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.archive.modules.CrawlURI;
import org.archive.modules.ProcessResult;
import org.archive.modules.Processor;
import org.archive.modules.net.ServerCache;
import org.archive.modules.revisit.IdenticalPayloadDigestRevisit;
import org.archive.util.ArchiveUtils;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;
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
public class DeDuplicator extends Processor {

    private static Logger logger =
        Logger.getLogger(DeDuplicator.class.getName());

    // General statistics
    /** Number of URIs that make it through the processors exclusion rules
     *  and are processed by it.
     */
    AtomicLong handledNumber = new AtomicLong(0);
    
    /** Number of URIs that are deemed duplicates
     */
    AtomicLong duplicateNumber = new AtomicLong(0);
    
    /** Then number of URIs that turned out to have exact URL and content 
     *  digest matches.
     */
    AtomicLong exactURLDuplicates = new AtomicLong(0);
    AtomicLong exactURLDuplicatesBytes = new AtomicLong(0);
    
    /** The number of URIs that turned out to have canonical URL and content
     *  digest matches. Does not include exact matches.
     */
    AtomicLong canonicalURLDuplicates = new AtomicLong(0);
    AtomicLong canonicalURLDuplicatesBytes = new AtomicLong(0);
    
    /** The number of URIs that, while having no exact or canonical matches,  
     *  do have exact content digest matches against other URIs.
     */
    AtomicLong digestDuplicates = new AtomicLong(0);
    AtomicLong digestDuplicatesBytes = new AtomicLong(0);
    
    /** The total amount of data represented by the documents who were deemed
     *  duplicates and excluded from further processing.
     */
    AtomicLong duplicateAmount = new AtomicLong(0);
    
    /** The total amount of data represented by all the documents processed **/
    AtomicLong totalAmount = new AtomicLong(0);

    /** Accumulated time spent doing lookups, in nanoseconds. Divide by handledNumber for average lookup time **/
    AtomicLong cumulativeLookupDuration = new AtomicLong(0);
    
    /** The number of nanoseconds the last lookup took. **/
    long lastLookupDuration = -1L;
    
    // Spring configurable parameters
    
    /* Index to use */
    Index index;
    public Index getIndex() {
        return index;
    }
    public void setIndex(Index index) {
        this.index=index;
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

	@Override
	protected boolean shouldProcess(CrawlURI curi) {
        if (curi.is2XXSuccess() == false) {
            // No point in doing comparison on failed downloads.
            logger.finest("Not handling " + curi.toString() + ", did not succeed.");
            return false;
        }
        if (curi.isHttpTransaction()==false) {
            // Non-http documents are not handled at present
            logger.finest("Not handling " + curi.toString() + ", non-http.");
            return false;
        }
        if(curi.isRevisit()){
            // A previous processor or filter has judged this CrawlURI to be a revisit
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

        handledNumber.incrementAndGet();
        totalAmount.addAndGet(curi.getContentSize());
        
        String url = curi.getURI();
        String canonicalizedURL = canonicalizer.canonicalize(url);
		String digest = curi.getContentDigestString();
        
		long beginLookup = System.nanoTime();
        IdenticalPayloadDigestRevisit duplicate = index.lookup(url, canonicalizedURL, digest);
        long lookupTook = System.nanoTime()-beginLookup;
        cumulativeLookupDuration.addAndGet(lookupTook);
        lastLookupDuration=lookupTook;

        if (duplicate != null){
        	// A little sanity check
        	if (duplicate.getPayloadDigest().equals(digest)==false) {
        		throw new IllegalStateException("Digest for CURI and duplicate does not match for " + curi.toString());
        	}
            // Increment statistics counters
            duplicateAmount.addAndGet(curi.getContentSize());
            duplicateNumber.incrementAndGet();
            count(duplicate, url, canonicalizedURL, curi.getContentLength());

            // Attach revisit profile to CURI. This will inform downstream processors that we've 
            // marked this as a duplicate/revisit
        	curi.setRevisitProfile(duplicate);

        	// Add annotation to crawl.log 
            curi.getAnnotations().add(REVISIT_ANNOTATION_MARKER);
            
            // Write extra logging information (needs to be enabled in CrawlerLoggerModule)
            curi.addExtraInfo(EXTRA_REVISIT_PROFILE, duplicate.getProfileName());
            curi.addExtraInfo(EXTRA_REVISIT_URI, duplicate.getRefersToTargetURI());
            curi.addExtraInfo(EXTRA_REVISIT_DATE, duplicate.getRefersToDate());
        }
        
        return ProcessResult.PROCEED;
	}
	
	private void count(IdenticalPayloadDigestRevisit dup, String url, String canonicalUrl, long contentLength) {
		if (dup.getRefersToTargetURI().equals(url)) {
			exactURLDuplicates.incrementAndGet();
			exactURLDuplicatesBytes.addAndGet(contentLength);
		} else if (canonicalizer.canonicalize(dup.getRefersToTargetURI()).equals(canonicalUrl)) {
			canonicalURLDuplicates.incrementAndGet();
			canonicalURLDuplicatesBytes.addAndGet(contentLength);
		} else {
			digestDuplicates.incrementAndGet();
			digestDuplicatesBytes.addAndGet(contentLength);
		}
	}
    
	public String report() {
        StringBuilder ret = new StringBuilder();
        ret.append("Processor: ");
        ret.append(DeDuplicator.class.getCanonicalName());
        ret.append("\n");
        ret.append("  Function:          Set revisit profile on records deemed duplicate by hash comparison\n");
        ret.append("  Total handled:     " + handledNumber + "\n");
        ret.append("  Duplicates found:  " + duplicateNumber + " " + 
        		getPercentage(duplicateNumber.get(),handledNumber.get()) + "\n");
        ret.append("  Bytes total:       " + totalAmount + " (" + 
        		ArchiveUtils.formatBytesForDisplay(totalAmount.get()) + ")\n");
        ret.append("  Bytes duplicte:    " + duplicateAmount + " (" + 
        		ArchiveUtils.formatBytesForDisplay(duplicateAmount.get()) + ") " + 
        		getPercentage(duplicateAmount.get(), totalAmount.get()) + "\n");
        
    	ret.append("  New (no hits):     " + (handledNumber.get()-
    			(digestDuplicates.get()+exactURLDuplicates.get()+
    			 canonicalURLDuplicates.get())) + "\n");
    	ret.append("  Exact URL hits:    " + exactURLDuplicates + "\n");
    	ret.append("  Exact URL bytes:   " + exactURLDuplicatesBytes);
    	ret.append(" (" + ArchiveUtils.formatBytesForDisplay(exactURLDuplicatesBytes.get()) + ")\n");
    	ret.append("  Canonical hits:    " + canonicalURLDuplicates + "\n");
    	ret.append("  Canonical bytes:   " + canonicalURLDuplicatesBytes);
    	ret.append(" (" + ArchiveUtils.formatBytesForDisplay(canonicalURLDuplicatesBytes.get()) + ")\n");
       	ret.append("  Digest hits:       " + digestDuplicates + "\n");
       	ret.append("  Digest bytes:      " + digestDuplicatesBytes);
    	ret.append(" (" + ArchiveUtils.formatBytesForDisplay(digestDuplicatesBytes.get()) + ")\n");
    	if (handledNumber.get()>0) {
	       	ret.append("  Average lookup time: " + String.format("%.3f", 
	       			(double)(cumulativeLookupDuration.get()/handledNumber.get())/1000000d)  + " ms\n");
	       	ret.append("  Last lookup time:    " + String.format("%.3f",(double)(lastLookupDuration/1000000d)) + " ms\n");
    	}
       	
       	ret.append("\n");
       	ret.append("Index:\n");
       	ret.append(index.getInfo());
        
        ret.append("\n");
        return ret.toString();
	}
	
	protected static String getPercentage(double portion, double total){
		NumberFormat percentFormat = NumberFormat.getPercentInstance(Locale.ENGLISH);
		percentFormat.setMaximumFractionDigits(1);
		return percentFormat.format(portion/total);
	}

}
