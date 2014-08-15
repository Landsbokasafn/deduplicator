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

import java.io.IOException;

/**
 * An abstract base class for implementations of iterators that iterate over
 * different sets of crawl data (i.e. crawl.log, ARC, WARC etc.)
 * 
 * @author Kristinn Sigur&eth;sson
 */
public interface CrawlDataIterator {
    /**
     * Prepare the iterator. Other methods may throw an exception until this method has been properly invoked.  
     * 
     * @param source The location of the crawl data. The meaning of this 
     *               value may vary based on the implementation of concrete
     *               subclasses. Typically it will refer to a directory or a
     *               file.
     * @throws IOException if an error occurs reading/preparing source data
     */
    public void initialize(String source) throws IOException;
    
    /**
     * Are there more elements?
     * @return true if there are more elements, false otherwise
     * @throws IOException If an error occurs accessing the crawl data.
     */
    public boolean hasNext() throws IOException;
    
    /**
     * Get the next {@link CrawlDataItem}.
     * @return the next CrawlDataItem. If there are no further elements then
     *         null will be returned.
     * @throws IOException If an error occurs accessing the crawl data.
     */
    public CrawlDataItem next() throws IOException;
    
    /**
     * Close any resources held open to read the crawl data.
     * @throws IOException If an error occurs closing access to crawl data.
     */
    public void close() throws IOException;
    
    /**
     * A short, human readable, string about what source this iterator uses.
     * I.e. "Iterator for Heritrix style crawl.log" etc. 
     * @return A short, human readable, string about what source this iterator 
     *         uses.
     */
    public String getSourceType();
}
