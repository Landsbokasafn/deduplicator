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
import java.util.HashMap;
import java.util.Iterator;
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
    
    /* Index to use */
    Index index;
    public Index getIndex() {
        return index;
    }
    public void setIndex(Index index) {
        this.index=index;
    }

    /* Should statistics be tracked per host? **/
   	boolean statsPerHost=false;
    public boolean getStatsPerHost(){
    	return statsPerHost;
    }
    public void setStatsPerHost(boolean statsPerHost){
    	this.statsPerHost=statsPerHost;
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
    protected Statistics stats = null;
    protected HashMap<String, Statistics> perHostStats = null;

    public void afterPropertiesSet() throws Exception {
        // Initialize statistics accumulators
        stats = new Statistics();
        if (statsPerHost) {
            perHostStats = new HashMap<String, Statistics>();
        }
    }
    

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

        stats.handledNumber.incrementAndGet();
        stats.totalAmount.addAndGet(curi.getContentSize());
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
            currHostStats.handledNumber.incrementAndGet();
            currHostStats.totalAmount.addAndGet(curi.getContentSize());
        }
        
        String url = curi.getURI();
        String canonicalizedURL = canonicalizer.canonicalize(url);
		String digest = curi.getContentDigestString();
        
        Duplicate duplicate = index.lookup(url, canonicalizedURL, digest); 
        
        if (duplicate != null){
            // Increment statistics counters
            stats.duplicateAmount.addAndGet(curi.getContentSize());
            stats.duplicateNumber.incrementAndGet();
            if(statsPerHost){ 
                currHostStats.duplicateAmount.addAndGet(curi.getContentSize());
                currHostStats.duplicateNumber.incrementAndGet();
            }
            count(duplicate, url, canonicalizedURL, currHostStats);

            // Attach revisit profile to CURI. This will inform downstream processors that we've 
            // marked this as a duplicate/revisit
        	IdenticalPayloadDigestRevisit revisitProfile = 
        			new IdenticalPayloadDigestRevisit(curi.getContentDigestString());
        	
        	revisitProfile.setRefersToTargetURI(duplicate.getUrl());
       		revisitProfile.setRefersToDate(duplicate.getDate());
        	
        	String refersToRecordID = duplicate.getWarcRecordId();
        	if (refersToRecordID!=null && !refersToRecordID.isEmpty()) {
        		revisitProfile.setRefersToRecordID(refersToRecordID);
        	}
        	
        	curi.setRevisitProfile(revisitProfile);

        	// Add annotation to crawl.log 
            curi.getAnnotations().add(REVISIT_ANNOTATION_MARKER);
            
            // Write extra logging information (needs to be enabled in CrawlerLoggerModule)
            curi.addExtraInfo(EXTRA_REVISIT_PROFILE, revisitProfile.getProfileName());
            curi.addExtraInfo(EXTRA_REVISIT_URI, revisitProfile.getRefersToTargetURI());
            curi.addExtraInfo(EXTRA_REVISIT_DATE, revisitProfile.getRefersToDate());
        }
        
        return ProcessResult.PROCEED;
	}

	private void count(Duplicate dup, String url, String canonicalUrl, Statistics currHostStats) {
		if (dup.getUrl().equals(url)) {
			stats.exactURLDuplicates.incrementAndGet();
			if (statsPerHost) {
				currHostStats.exactURLDuplicates.incrementAndGet();
			}
		} else if (canonicalizer.canonicalize(dup.getUrl()).equals(canonicalUrl)) {
			stats.canonicalURLDuplicates.incrementAndGet();
			if (statsPerHost) {
				currHostStats.canonicalURLDuplicates.incrementAndGet();
			}
		} else {
			stats.digestDuplicates.incrementAndGet();
			if (statsPerHost) {
				currHostStats.digestDuplicates.incrementAndGet();
			}
		}
	}
    
	public String report() {
        StringBuilder ret = new StringBuilder();
        ret.append("Processor: ");
        ret.append(DeDuplicator.class.getCanonicalName());
        ret.append("\n");
        ret.append("  Function:          Set revisit profile on records deemed duplicate by hash comparison\n");
        ret.append("  Total handled:     " + stats.handledNumber + "\n");
        ret.append("  Duplicates found:  " + stats.duplicateNumber + " " + 
        		getPercentage(stats.duplicateNumber.get(),stats.handledNumber.get()) + "\n");
        ret.append("  Bytes total:       " + stats.totalAmount + " (" + 
        		ArchiveUtils.formatBytesForDisplay(stats.totalAmount.get()) + ")\n");
        ret.append("  Bytes duplicte:    " + stats.duplicateAmount + " (" + 
        		ArchiveUtils.formatBytesForDisplay(stats.duplicateAmount.get()) + ") " + 
        		getPercentage(stats.duplicateAmount.get(), stats.totalAmount.get()) + "\n");
        
    	ret.append("  New (no hits):     " + (stats.handledNumber.get()-
    			(stats.digestDuplicates.get()+stats.exactURLDuplicates.get()+
    			 stats.canonicalURLDuplicates.get())) + "\n");
    	ret.append("  Exact hits:        " + stats.exactURLDuplicates + "\n");
    	ret.append("  Canonical hits:    " + stats.canonicalURLDuplicates + "\n");
       	ret.append("  Digest hits:       " + stats.digestDuplicates + "\n");
        
        if(statsPerHost){
            ret.append("  [Host] [total] [duplicates] [bytes] " +
                    "[bytes discarded] [new] [exact] [canon] [digest]");
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
                    ret.append(curr.handledNumber.get()-
                            (curr.digestDuplicates.get()+
                             curr.exactURLDuplicates.get()+
                             curr.canonicalURLDuplicates.get()));
                    ret.append(" ");
                    ret.append(curr.exactURLDuplicates);
                    ret.append(" ");
                    ret.append(curr.canonicalURLDuplicates);
                    ret.append(" ");
                    ret.append(curr.digestDuplicates);
                    ret.append("\n");
                }
            }
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

class Statistics{
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
    
    /** The number of URIs that turned out to have canonical URL and content
     *  digest matches. Does not include exact matches.
     */
    AtomicLong canonicalURLDuplicates = new AtomicLong(0);
    
    /** The number of URIs that, while having no exact or canonical matches,  
     *  do have exact content digest matches against other URIs.
     */
    AtomicLong digestDuplicates = new AtomicLong(0);
    
    /** The total amount of data represented by the documents who were deemed
     *  duplicates and excluded from further processing.
     */
    AtomicLong duplicateAmount = new AtomicLong(0);
    
    /** The total amount of data represented by all the documents processed **/
    AtomicLong totalAmount = new AtomicLong(0);

}
