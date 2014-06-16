package org.muehleisen.hannes.naward2014.tests;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;
import org.muehleisen.hannes.naward2014.ccTldFinder;

public class ccTldFinderTest {

	@Test
	public void udfTest() throws IOException {
		Tuple t = TupleFactory.getInstance().newTuple(1);
		ccTldFinder cdf = new ccTldFinder();
		t.set(0, "http://www.example.com/asdf");
		assertEquals("", cdf.exec(t));
		t.set(0, "http://www.example.de/asdf");
		assertEquals("DE", cdf.exec(t));
		t.set(0, "http://www.example.co.uk/asdf");
		assertEquals("UK", cdf.exec(t));
	}
}
