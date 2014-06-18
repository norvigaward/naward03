package org.muehleisen.hannes.naward2014.tests;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;
import org.muehleisen.hannes.naward2014.CountryBlackMagic;

public class CountryBlackMagicTest {

	@Test
	public void udfTest() throws IOException {
		Tuple t = TupleFactory.getInstance().newTuple(3);
		CountryBlackMagic cbm = new CountryBlackMagic();

		t.set(0, "80.237.133.40"); // iploc = DE
		t.set(1, "http://www.example.de/asdf"); // cctld = DE
		t.set(2, "Das Pferd frisst keinen Gurkensalat."); // plaintextlang = DE

		assertEquals("DE", cbm.exec(t));

		t.set(0, "172.229.108.110"); // iploc = US
		t.set(1, "http://www.example.com/asdf"); // cctld = NA
		t.set(2, "Das Pferd frisst keinen Gurkensalat."); // plaintextlang = DE

		assertEquals(null, cbm.exec(t));
	}
}
