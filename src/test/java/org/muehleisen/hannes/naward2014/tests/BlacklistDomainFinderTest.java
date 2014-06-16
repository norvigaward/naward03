package org.muehleisen.hannes.naward2014.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;
import org.muehleisen.hannes.naward2014.BlacklistDomainFinder;

public class BlacklistDomainFinderTest {
    @Test
	public void pldTest() {
    	assertEquals("example.com",BlacklistDomainFinder.getPld("www.example.com"));
    	assertEquals("example.co.uk",BlacklistDomainFinder.getPld("www.example.co.uk"));
    	assertEquals("example.com",BlacklistDomainFinder.getPld("subsub.sub.example.com"));
	}
    
    @Test
    public void udfTest() throws IOException {
    	Tuple t = TupleFactory.getInstance().newTuple(1);
		BlacklistDomainFinder bdf = new BlacklistDomainFinder();
		t.set(0,"http://www.zytrog.com/asdf");
		assertTrue(bdf.exec(t));
		t.set(0,"https://zytrog.com/");
		assertTrue(bdf.exec(t));
		t.set(0,"http://donaldduck.disney.com/cv");
		assertFalse(bdf.exec(t));
		t.set(0,"http://disney.com");
		assertFalse(bdf.exec(t));
    }
}
