package is.landsbokasafn.deduplicator;

import junit.framework.TestCase;

public class DeDuplicatorTest extends TestCase {

	public void testGetPercentage() throws Exception{
		assertEquals("2.5%",DeDuplicator.getPercentage(5,200));
	}
    
}
