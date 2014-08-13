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
        assertEquals("YA3G7O6TNMHXA5WWDSIZJDNXV56WDRCA", tmp.getContentDigest());

        cli.close();
        
        testFile.delete(); //Cleanup
    }
}
