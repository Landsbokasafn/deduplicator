package is.landsbokasafn.deduplicator.heritrix;

import static is.landsbokasafn.deduplicator.IndexFields.DATE;
import static is.landsbokasafn.deduplicator.IndexFields.DIGEST;
import static is.landsbokasafn.deduplicator.IndexFields.ORIGINAL_RECORD_ID;
import static is.landsbokasafn.deduplicator.IndexFields.URL;
import static is.landsbokasafn.deduplicator.IndexFields.URL_CANONICALIZED;
import static is.landsbokasafn.deduplicator.heritrix.DuplicateType.CANONICAL_URL;
import static is.landsbokasafn.deduplicator.heritrix.DuplicateType.DIGEST_ONLY;
import static is.landsbokasafn.deduplicator.heritrix.DuplicateType.EXACT_URL;
import static is.landsbokasafn.deduplicator.heritrix.SearchStrategy.*;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.NIOFSDirectory;
import org.springframework.beans.factory.InitializingBean;

public class LuceneIndexSearcher implements InitializingBean {
    private static Logger logger = Logger.getLogger(LuceneIndexSearcher.class.getName());

    protected IndexSearcher searcher = null;
    protected DirectoryReader dReader = null;
    
    protected boolean urlIndexed = false;  // Is the URL field indexed
    protected boolean digestIndexed = false; // Is the Digest field indexed
    protected boolean canoncialAvailable = false; // Is the URL_Canonicalized field present. Indexed if URL is.

    private String indexLocation;
    /**
     * Set the location of the index in the filesystem. Changing this value after the bean has been 
     * initialized will have no effect.
     * @param indexLocation The location of the index.
     */
    public void setIndexLocation(String indexLocation) {
        this.indexLocation=indexLocation;
    }
    public String getIndexLocation() {
        return indexLocation;
    }

    protected SearchStrategy strategy;
    /**
     * Set the search strategy to employ.
     *  
     * @param strategy The search strategy to employ
     * @see SearchStrategy
     */
	public void setStrategy(SearchStrategy strategy) {
		if (searcher!=null) {
			verifyStrategy(strategy);
		}
		this.strategy = strategy;
	}
	public SearchStrategy getStrategy() {
		return strategy;
	}
	
	
	@Override
	public void afterPropertiesSet() throws Exception {
    	openIndex(indexLocation);
    	verifyStrategy(strategy);
    }
    
    private void openIndex(String indexLocation) {
    	if (searcher!=null) {
    		throw new IllegalStateException("Already have an index");
    	}
    	try {
            dReader = DirectoryReader.open(new NIOFSDirectory(new File(indexLocation)));
            searcher = new IndexSearcher(dReader);
        } catch (Exception e) {
        	throw new IllegalArgumentException("Unable to find/open index at " + indexLocation,e);
        } 
    	inspectIndex();
    }
    
    private void inspectIndex() {
    	// Determine index makeup
        urlIndexed = isFieldIndexed(URL.name());
        digestIndexed = isFieldIndexed(DIGEST.name());
        if (urlIndexed==false && digestIndexed==false) {
        	throw new IllegalStateException("Either URL or DIGEST fields must be indexed (or both).");
        }
        try {
            boolean canonicalIndexed = isFieldIndexed(URL_CANONICALIZED.name());
            if (canonicalIndexed==urlIndexed) {
            	canoncialAvailable=true;
            } else {
            	logger.severe("URL_CANONICALIZED and URL fields disagree on indexing. "
            			+ "Either both must be indexed or neither. Proceeding as if URL_CANONICALIZED "
            			+ "was not available.");
            }
        } catch (NullPointerException e) {
        	canoncialAvailable=false;
        }
    }
    
    /**
     * Verify that the current index supports the selected strategy. I.e. that the necessary fields are indexed.
     * @param strategy The strategy to verify
     * @throws IllegalStateException if the strategy can not be carried out for the current index
     */
    private void verifyStrategy(SearchStrategy strategy) {
    	switch (strategy) {
    	case URL_EXACT :
    	case URL_CANONICAL :
    	case URL_CANONICAL_FALLBACK :
    		// All three require only that the url be indexed
    		if (!urlIndexed) {
    			throw new IllegalStateException("URL must be indexed for search strategy " + strategy.name());
    		}
    		break;
    	case DIGEST_ANY :
    		if (!digestIndexed) {
    			throw new IllegalStateException("Digest must be indexed for search strategy " + strategy.name());
    		}
    		break;
    	case URL_DIGEST_FALLBACK :
    		if (!urlIndexed || !digestIndexed) {
    			throw new IllegalStateException("URL and DIGEST must be indexed for search strategy " + 
    					strategy.name());
    		}
    		break;
    	}
    	if (strategy.equals(URL_CANONICAL_FALLBACK) && !canoncialAvailable){
    		throw new IllegalStateException(URL_CANONICAL_FALLBACK + 
    				" search requires that canonical url be in the index.");
    	}
    }
    
    protected boolean isFieldIndexed(String field) {
        IndexReader reader = searcher.getIndexReader();
        for (AtomicReaderContext rc : reader.leaves()) {
	        AtomicReader ar = rc.reader();
	        FieldInfos fis = ar.getFieldInfos();
	        if (!fis.fieldInfo(field).isIndexed()) {
	        	// All leaves must agree for us to return true
	        	return false;
	        }
        }
    	return true;
    }
    
    public Duplicate lookup(String url, String canonicalizedURL, String digest) {
    	switch (strategy) {
		case URL_EXACT:
			return lookupExactUrl(url, digest);
		case URL_CANONICAL_FALLBACK:
			return lookupCanonicalFallback(url, canonicalizedURL, digest);
		case URL_CANONICAL:
			return lookupCanonical(canonicalizedURL, digest);
		case URL_DIGEST_FALLBACK:
			break;
		case DIGEST_ANY:
			return lookupDigest(digest);
    	}
    	return null;
    }
    
    protected Duplicate lookupExactUrl(final String url, final String digest) {
    	Document doc = lookupUrl(url, digest, URL.name());
    	if (doc!=null) {
    		return wrap(doc,EXACT_URL);
    	}
    	return null;
    }
    
    protected Duplicate lookupCanonical(String canonicalizedURL, String digest) {
    	Document doc = lookupUrl(canonicalizedURL, digest, URL_CANONICALIZED.name());
    	if (doc!=null) {
    		return wrap(doc,CANONICAL_URL);
    	}
    	return null;
    }
    
    protected Duplicate lookupCanonicalFallback(String url, String canonicalizedURL, String digest) {
    	Duplicate dup = lookupExactUrl(url, digest);
    	if (dup==null) {
    		dup = lookupCanonical(canonicalizedURL, digest);
    	}
    	return dup;
    }
    
	protected Duplicate wrap(Document doc, DuplicateType type) {
		Duplicate dup = new Duplicate();
		dup.setUrl(doc.get(URL.name()));
		dup.setType(type);
		dup.setDate(doc.get(DATE.name()));
		dup.setWarcRecordId(doc.get(ORIGINAL_RECORD_ID.name()));
		return dup;
	}

	private Document lookupUrl(final String url, final String digest, final String field) {
		try {
			Query query = null;
			if (digestIndexed) {
            	BooleanQuery q = new BooleanQuery();
            	q.add(new TermQuery(new Term(field, url)), Occur.MUST);
            	q.add(new TermQuery(new Term(DIGEST.name(), digest)), Occur.MUST);
				query = q;
			} else {
				query = new TermQuery(new Term(field,url));
			}
			
			ScoreDoc[] hits = searcher.search(query, null, 5).scoreDocs;

			Document doc = null;
			if (hits != null && hits.length > 0) {
				for (int i = 0; i < hits.length; i++) {
					doc = searcher.doc(hits[i].doc);
					String oldDigest = doc.get(DIGEST.name());

					if (oldDigest.equalsIgnoreCase(digest)) {
						// If we found a hit, no need to look at other hits.
						return doc;
					}
				}
			}
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Error accessing index.", e);
		}
		return null; // Didn't find a match
	}
	
	protected Duplicate lookupUrlDigestFallback(String url, String canonicalizedURL, String digest) {
		Duplicate dup = lookupCanonicalFallback(url, canonicalizedURL, digest);
		if (dup==null) {
			dup = lookupDigest(digest);
		}
		return dup;
	}

    protected Duplicate lookupDigest(final String digest) {
        Duplicate duplicate = null; 
        Query query = new TermQuery(new Term(DIGEST.name(), digest));
        try {
            ScoreDoc[] hits = searcher.search(query, null, 1).scoreDocs; 
            if(hits != null && hits.length > 0){
            	// May be multiple hits, but all hits are equal as far as we are concerned.
                duplicate = wrap(searcher.doc(hits[0].doc), DIGEST_ONLY);
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE,"Error accessing index.",e);
        }
        return duplicate;
    }
	
}
