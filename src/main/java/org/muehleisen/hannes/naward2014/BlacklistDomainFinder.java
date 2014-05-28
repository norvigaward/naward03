package org.muehleisen.hannes.naward2014;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class BlacklistDomainFinder extends EvalFunc<Boolean> {
	private static final Set<String> baddomains = new HashSet<String>();

	static {
		// load known p0rn domain list
		BufferedReader rdr = new BufferedReader(new InputStreamReader(BlacklistDomainFinder.class.
				getClassLoader().getResourceAsStream("domains")));
		String sw = null;
		try {
			while ((sw = rdr.readLine()) != null) {
				baddomains.add(sw.trim());
			}
		} catch (IOException e) {
		}
	}

	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() < 1)
			return false;
		try {
			// find out whether the domain itself is in the blacklist
			// find out whether its pld is in the blacklist

		} catch (Exception e) {
			warn(e.getMessage(), PigWarning.UDF_WARNING_1);
		}
		return false;
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.BOOLEAN));
	}
}