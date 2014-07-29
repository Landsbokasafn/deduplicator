package is.landsbokasafn.deduplicator;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpParser;
import org.apache.commons.httpclient.StatusLine;
import org.apache.commons.httpclient.util.EncodingUtil;
import org.archive.format.warc.WARCConstants;
import org.archive.format.warc.WARCConstants.WARCRecordType;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCRecord;

public class WarcFileIterator extends CrawlDataIterator {

	private File warcFile;
    private Iterator<ArchiveRecord> recordIterator = null;
    private WARCReader reader = null;
    
    private CrawlDataItem nextItem = null;
	
	public WarcFileIterator(String source) throws IOException {
		super(source);
		warcFile = new File(source);
		if (!warcFile.exists()) {
			throw new IllegalArgumentException("No such file " + warcFile.getAbsolutePath());
		}
		readNextItem();
	}
	
	private void readNextItem() throws IOException {
		// Invalidate any previous items
		nextItem=null;
		// Open file if needed
		if (reader==null) {
			try {
		        reader = WARCReaderFactory.get(warcFile);
		        recordIterator = reader.iterator();
			} catch (IOException e) {
				System.out.println("Failed to open and read " + warcFile.getAbsolutePath());
				return;
			}
		}
		// Find next response record
		while (recordIterator.hasNext() && nextItem==null) {
	        WARCRecord record = (WARCRecord)recordIterator.next();
	        ArchiveRecordHeader header = record.getHeader();
	        
	        if (header.getUrl()==null || !header.getUrl().startsWith("http")) {
	        	continue;
	        }
	        
	        WARCRecordType type = WARCRecordType.valueOf(
	        		header.getHeaderValue(WARCConstants.HEADER_KEY_TYPE).toString());
	        
	        switch (type) {
	        	case response :
	        		nextItem = processResponse(record, header);
	        		break;
	        	case revisit :
	        		nextItem = processRevisit(record, header);
	        		break;
	        	default:
	        		// For anything else, do nothing
	        }
		}
		if (!recordIterator.hasNext()) {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			} 
		}
	}

	private CrawlDataItem processResponse(WARCRecord record, ArchiveRecordHeader header) throws IOException {
		CrawlDataItem cdi = new CrawlDataItem();
		cdi.setURL(header.getUrl());
		cdi.setContentDigest((String)header.getHeaderValue(WARCConstants.HEADER_KEY_PAYLOAD_DIGEST));
		cdi.setRevisit(false);
		cdi.setTimestamp(header.getDate());
		cdi.setWarcRecordId((String)header.getHeaderValue(WARCConstants.HEADER_KEY_ID));
		
		// Process the HTTP header, if any
        byte [] statusBytes = HttpParser.readRawLine(record);
        int eolCharCount = getEolCharsCount(statusBytes);
        if (eolCharCount > 0) {
	        String statusLine = EncodingUtil.getString(statusBytes, 0,
	            statusBytes.length - eolCharCount, WARCConstants.DEFAULT_ENCODING);
	        if ((statusLine != null) && StatusLine.startsWithHTTP(statusLine)) {
	            StatusLine status = new StatusLine(statusLine);
	    		cdi.setStatusCode(status.getStatusCode());
	    		Header[] headers = HttpParser.parseHeaders(record,WARCConstants.DEFAULT_ENCODING);
                for (Header h : headers) {
                	if (h.getName().equalsIgnoreCase("Content-Type")) {
                		cdi.setMimeType(h.getValue());
                	} else if (h.getName().equalsIgnoreCase("ETag")) {
                    	cdi.setEtag(h.getValue());
                    }
                }
	        }
        }
		
		
		return cdi;
	}
	
	private CrawlDataItem processRevisit(WARCRecord record, ArchiveRecordHeader header) throws IOException{
		CrawlDataItem cdi = processResponse(record, header);
		cdi.setOriginalURL((String)header.getHeaderValue(WARCConstants.HEADER_KEY_REFERS_TO_TARGET_URI));
		cdi.setOriginalTimestamp((String)header.getHeaderValue(WARCConstants.HEADER_KEY_REFERS_TO_DATE));
		cdi.setRevisitProfile((String)header.getHeaderValue(WARCConstants.HEADER_KEY_PROFILE));
		if (!cdi.getRevisitProfile().equals(WARCConstants.PROFILE_REVISIT_NOT_MODIFIED)) {
			// ETags are of questionable value in this scenario, null it out, if any
			cdi.setEtag(null);
		}

		cdi.setRevisit(true);
		
		return cdi;
	}

	
	@Override
	public boolean hasNext() {
		return nextItem!=null;
	}

	@Override
	public CrawlDataItem next() throws IOException {
		CrawlDataItem next = nextItem;
		readNextItem();
		return next;
	}

	@Override
	public void close() throws IOException {
		if (reader!=null) {
			reader.close();
		}
	}
	
    /**
     * @param bytes Array of bytes to examine for an EOL.
     * @return Count of end-of-line characters or zero if none.
     * 
     * Borrowed from {@link org.archive.io.arc.ARCRecord}
     */
    private int getEolCharsCount(byte [] bytes) {
        int count = 0;
        if (bytes != null && bytes.length >=1 &&
                bytes[bytes.length - 1] == '\n') {
            count++;
            if (bytes.length >=2 && bytes[bytes.length -2] == '\r') {
                count++;
            }
        }
        return count;
    }

	@Override
	public String getSourceType() {
		return "Iterator over a single WARC (ISO-28500) file.";
	}

}
