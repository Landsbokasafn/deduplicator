package is.landsbokasafn.deduplicator;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

public class WarcIterator extends CrawlDataIterator {

	public static final String warcFileRegex = "^.*\\.warc(.gz)?$";
	
	private List<File> warcFiles;
	private Iterator<File> fileIterator = null;
    private WarcFileIterator recordIterator = null;
    
    private CrawlDataItem nextItem = null;
    
	public WarcIterator(String source) throws IOException {
		super(source);
		// Source is required to be a valid directory. Scan it and all sub-folders for WARC files
		File baseDir = new File(source);
		if (!baseDir.exists()) {
			throw new IllegalArgumentException(source + " is not a valid directory");
		}
		warcFiles = new LinkedList<File>();
		addWarcsInDir(baseDir);
		Collections.sort(warcFiles);
		fileIterator=warcFiles.iterator();
		readNextItem();
	}
	
	private void addWarcsInDir(File dir) {
		for (File f : dir.listFiles()) {
			if (f.isDirectory()) {
				addWarcsInDir(f);
			} else if (f.getName().matches(warcFileRegex)) {
				warcFiles.add(f);
			}
			
		}
	}

	private void readNextItem() throws IOException {
		// Invalidate previous item
		nextItem = null;
		// Open new record iterator if needed
		while (recordIterator==null || !recordIterator.hasNext()) {
			if (fileIterator.hasNext()) {
				File warcFile = fileIterator.next();
				System.out.println("Opening up " + warcFile.getAbsolutePath());			
				recordIterator=new WarcFileIterator(warcFile.getAbsolutePath());
			} else {
				return;
			}
		}
		nextItem=recordIterator.next();
	}
	
	@Override
	public boolean hasNext() throws IOException {
		return nextItem!=null;
	}

	@Override
	public CrawlDataItem next() throws IOException {
		if (!this.hasNext()) {
			throw new NoSuchElementException();
		}
		CrawlDataItem cdi = nextItem;
		// Advance the iterator
		readNextItem();
		
		return cdi;
	}

	@Override
	public void close() throws IOException {
		// No action needed
	}

	@Override
	public String getSourceType() {
		return "Iterator over all WARC (ISO-28500) files in a directory (recursive).";
	}

}
