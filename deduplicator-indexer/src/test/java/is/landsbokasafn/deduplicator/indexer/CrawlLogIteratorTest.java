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

import junit.framework.TestCase;

public class CrawlLogIteratorTest extends TestCase {
    public void testParseLine() throws IOException{
        File testFile = new File("test");
        testFile.createNewFile();
        CrawlLogIterator cli = new CrawlLogIterator();
        String lineValidWithoutAnnotation = 
            "2006-10-17T14:22:29.343Z   200      29764 http://www.bok.hi.is/image.gif E http://www.bok.hi.is/ image/gif #008 20061017142229253+74 YA3G7O6TNMHXA5WWDSIZJDNXV56WDRCA - -";
        String lineValidWithoutOrigin = 
            "2006-10-17T14:22:29.391Z   200       7951 http://www.bok.hi.is/ X http://bok.hi.is/ text/html #029 20061017142228950+364 SBRY3NIKXYAIKSCJ5QL2F6AE4GG7P6VR - 3t";
        String lineValidWithOrigin = 
            "2006-10-17T14:22:29.399Z   200      18803 http://www.bok.hi.is/ X http://bok.hi.is/ text/html #041 20061017142229087+180 OHCVML7NJ4STPQSRRWY7WWJL6T5H2R6L - duplicate:\"ORIGIN\",3t";
        String lineTruncated =
            "2006-10-17T14:22:29.399Z   200      18803 http://www.bok.hi.is/ X http://bok.hi.";
        String lineValidWithDigestPrefix = 
            "2006-10-17T14:22:29.343Z   200      29764 http://www.bok.hi.is/image.gif E http://www.bok.hi.is/ image/gif #008 20061017142229253+74 sha1:YA3G7O6TNMHXA5WWDSIZJDNXV56WDRCA - -";

        // TODO: UPDATE THESE TO TEST FOR THE NEW ANNOTATIONS!!!!
        
        CrawlDataItem tmp = 
            cli.parseLine(lineValidWithoutAnnotation);
        assertNotNull(tmp);
        assertEquals(200, tmp.getStatusCode());
        
        tmp = cli.parseLine(lineValidWithoutOrigin);
        assertNotNull(tmp);
//        assertNull(tmp.getOrigin());
        
        tmp = cli.parseLine(lineValidWithOrigin);
        assertNotNull(tmp);
//        assertEquals("ORIGIN", tmp.getOrigin());
        
        tmp = cli.parseLine(lineTruncated);
        assertNull(tmp);
        
        tmp = cli.parseLine(lineValidWithDigestPrefix);
        assertEquals("sha1:YA3G7O6TNMHXA5WWDSIZJDNXV56WDRCA", tmp.getContentDigest());

        cli.close();
        
        testFile.delete(); //Cleanup
    }
}
