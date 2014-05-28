package org.muehleisen.hannes.naward2014;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.net.InternetDomainName;

public class BlacklistDomainFinder extends EvalFunc<Boolean> {
	private static final Set<String> baddomains = new HashSet<String>();

	static String getPld(String domain) {
		InternetDomainName idn = InternetDomainName.from(domain);
		while (!idn.isTopPrivateDomain()) {
			idn = idn.parent();
		}
		return idn.toString();
	}

	static {
		// load known p0rn domain list
		BufferedReader rdr = new BufferedReader(new InputStreamReader(
				BlacklistDomainFinder.class.getClassLoader()
						.getResourceAsStream("domains")));
		String sw = null;
		try {
			while ((sw = rdr.readLine()) != null) {
				try {
					String fdomain = sw.trim();
					if (!baddomains.contains(fdomain)) {
						baddomains.add(fdomain);
					}
					String pdomain = getPld(fdomain);
					if (!baddomains.contains(pdomain)) {
						baddomains.add(pdomain);
					}
				} catch (Exception e) {
				}
			}
		} catch (IOException e) {
		}
	}

	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() < 1)
			return false;
		try {
			// find out whether the domain itself is in the blacklist
			URI u = new URI(((String) input.get(0)).trim());
			String fdomain = u.getHost();
			if (baddomains.contains(fdomain)) {
				return true;
			}
			// find out whether its pld is in the blacklist
			String pdomain = getPld(fdomain);
			if (baddomains.contains(pdomain)) {
				return true;
			}
			return false;
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