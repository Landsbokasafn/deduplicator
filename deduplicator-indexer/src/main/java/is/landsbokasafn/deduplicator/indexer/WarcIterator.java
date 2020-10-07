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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

public class WarcIterator implements CrawlDataIterator {

	public static final String WARC_FILE_REGEX = "^.*\\.warc(.gz)?$";
	
	private List<File> warcFiles;
	private Iterator<File> fileIterator = null;
    private WarcFileIterator recordIterator = null;
    
    private CrawlDataItem nextItem = null;
    
    public WarcIterator(){
    	
    }
    
    /**
     * Convenience constructor. Equivalent to using no-arg constructor and than invoking initialize with the 
     * same parameter.
     * @param source The folder containing WARC files to iterate over
     * @throws IOException If an error occurs reading the source
     */
    public WarcIterator(String source) throws IOException {
    	initialize(source);
    }
    
	public void initialize(String source) throws IOException {
		// Source is required to be a valid directory. Scan it and all sub-folders for WARC files
		File baseDir = new File(source);
		if (!baseDir.exists()) {
			throw new IllegalArgumentException(source + " is not a valid directory");
		}
		warcFiles = new LinkedList<>();
		addWarcsInDir(baseDir);
		Collections.sort(warcFiles);
		fileIterator=warcFiles.iterator();
		readNextItem();
	}
	
	private void addWarcsInDir(File dir) {
		for (File f : dir.listFiles()) {
			if (f.isDirectory()) {
				addWarcsInDir(f);
			} else if (f.getName().matches(WARC_FILE_REGEX)) {
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
