package org.muehleisen.hannes.naward2014;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.net.InternetDomainName;

public class ccTldFinder extends EvalFunc<String> {

	// list of known ccTLDs from IANA, 2014-06-16
	public static final String[] ccTLDsVal = new String[] { "ac", "ad", "ae",
			"af", "ag", "ai", "al", "am", "an", "ao", "aq", "ar", "as", "at",
			"au", "aw", "ax", "az", "ba", "bb", "bd", "be", "bf", "bg", "bh",
			"bi", "bj", "bl", "bm", "bn", "bo", "bq", "br", "bs", "bt", "bv",
			"bw", "by", "bz", "ca", "cc", "cd", "cf", "cg", "ch", "ci", "ck",
			"cl", "cm", "cn", "co", "cr", "cu", "cv", "cw", "cx", "cy", "cz",
			"de", "dj", "dk", "dm", "do", "dz", "ec", "ee", "eg", "eh", "er",
			"es", "et", "eu", "fi", "fj", "fk", "fm", "fo", "fr", "ga", "gb",
			"gd", "ge", "gf", "gg", "gh", "gi", "gl", "gm", "gn", "gp", "gq",
			"gr", "gs", "gt", "gu", "gw", "gy", "hk", "hm", "hn", "hr", "ht",
			"hu", "id", "ie", "il", "im", "in", "io", "iq", "ir", "is", "it",
			"je", "jm", "jo", "jp", "ke", "kg", "kh", "ki", "km", "kn", "kp",
			"kr", "kw", "ky", "kz", "la", "lb", "lc", "li", "lk", "lr", "ls",
			"lt", "lu", "lv", "ly", "ma", "mc", "md", "me", "mf", "mg", "mh",
			"mk", "ml", "mm", "mn", "mo", "mp", "mq", "mr", "ms", "mt", "mu",
			"mv", "mw", "mx", "my", "mz", "na", "nc", "ne", "nf", "ng", "ni",
			"nl", "no", "np", "nr", "nu", "nz", "om", "pa", "pe", "pf", "pg",
			"ph", "pk", "pl", "pm", "pn", "pr", "ps", "pt", "pw", "py", "qa",
			"re", "ro", "rs", "ru", "rw", "sa", "sb", "sc", "sd", "se", "sg",
			"sh", "si", "sj", "sk", "sl", "sm", "sn", "so", "sr", "ss", "st",
			"su", "sv", "sx", "sy", "sz", "tc", "td", "tf", "tg", "th", "tj",
			"tk", "tl", "tm", "tn", "to", "tp", "tr", "tt", "tv", "tw", "tz",
			"ua", "ug", "uk", "um", "us", "uy", "uz", "va", "vc", "ve", "vg",
			"vi", "vn", "vu", "wf", "ws", "ye", "yt", "za", "zm", "zw" };

	public static final Set<String> ccTLDs = new HashSet<String>(
			Arrays.asList(ccTLDsVal));

	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() < 1)
			return "";
		try {
			// find out whether the domain itself is in the blacklist
			InternetDomainName idn = InternetDomainName.from(new URI(
					((String) input.get(0)).trim()).getHost());

			List<String> dParts = idn.parts();
			String tld = dParts.get(dParts.size() - 1).toLowerCase();
			if (ccTLDs.contains(tld)) {
				return tld.toUpperCase();
			} else {
				return "";
			}
		} catch (Exception e) {
			warn(e.getMessage(), PigWarning.UDF_WARNING_1);
		}
		return "";
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
	}

}