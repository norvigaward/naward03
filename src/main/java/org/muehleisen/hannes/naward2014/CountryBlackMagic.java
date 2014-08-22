package org.muehleisen.hannes.naward2014;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.google.common.net.InternetDomainName;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CountryResponse;

public class CountryBlackMagic extends EvalFunc<String> {
	private static final Map<String, Set<String>> langCountryMap = new HashMap<String, Set<String>>();

	static {
		// load known p0rn domain list
		BufferedReader rdr = new BufferedReader(new InputStreamReader(
				CountryBlackMagic.class.getClassLoader().getResourceAsStream(
						"countrylangs.tsv")));
		String sw = null;
		try {
			while ((sw = rdr.readLine()) != null) {
				String parts[] = sw.split("\t");
				String lang = parts[0];
				String country = parts[1];
				if (!langCountryMap.containsKey(lang)) {
					langCountryMap.put(lang, new HashSet<String>());
				}
				langCountryMap.get(lang).add(country);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static DatabaseReader db;
	static {
		try {
			db = new DatabaseReader.Builder(new GZIPInputStream(
					CountryBlackMagic.class.getClassLoader()
							.getResourceAsStream("GeoLite2-Country.mmdb.gz")))
					.build();
		} catch (IOException e) {
		}
	}

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

	static {
		File tempdir;
		try {
			// This is ugly, but the langdetect library insists on a folder with
			// its profiles, so we load a ZIP file containing them from the
			// classpath, extract to a temporary directory, and have them loaded
			// from there. Yes, to make a temporary directory we first create a
			// temporary file, then delete it, and then abuse its name for the
			// new directory. Ugly again, but hopefully effective.
			tempdir = File.createTempFile("langdetect-profiles-",
					Long.toString(System.nanoTime()));
			tempdir.delete();
			tempdir.mkdir();

			InputStream zipfile = CountryBlackMagic.class
					.getResourceAsStream("profiles.zip");
			unZip(zipfile, tempdir);

			DetectorFactory.loadProfile(tempdir);
			deleteFolder(tempdir);

		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}

	public static void increment(Map<String, Integer> map, String key) {
		if (key == null || "".equals(key.trim())) {
			return;
		}
		if (!map.containsKey(key)) {
			map.put(key, 0);
		}
		map.put(key, map.get(key) + 1);
	}

	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() < 3)
			return null;
		String ip = ((String) input.get(0)).trim();
		String url = ((String) input.get(1)).trim();
		String plaintext = ((String) input.get(2)).trim();

		Map<String, Integer> countryVotes = new HashMap<String, Integer>();

		// ip geolocation
		try {
			CountryResponse cres = db.country(InetAddress.getByName(ip));
			increment(countryVotes, cres.getCountry().getIsoCode()
					.toUpperCase());
		} catch (Exception e) {
			//e.printStackTrace();
		}

		// ccTLD
		try {
			InternetDomainName idn = InternetDomainName.from(new URI(url)
					.getHost());
			List<String> dParts = idn.parts();
			String tld = dParts.get(dParts.size() - 1).toLowerCase();
			if (ccTLDs.contains(tld)) {
				increment(countryVotes, tld.toUpperCase());
			}
		} catch (Exception e) {
			//e.printStackTrace();
		}

		// content language
		try {
			Detector detector = DetectorFactory.create();
			detector.append(plaintext);
			String detectedLang = detector.detect().toUpperCase();
			Set<String> countryCandidates = langCountryMap.get(detectedLang);
			if (countryCandidates != null) {
				for (String candidate : countryCandidates) {
					increment(countryVotes, candidate.toUpperCase());
				}
			}
		} catch (Exception e) {
			//e.printStackTrace();
		}

		int maxVotes = 0;
		String maxKey = null;

		for (Entry<String, Integer> e : countryVotes.entrySet()) {
			if (e.getValue() > maxVotes) {
				maxVotes = e.getValue();
				maxKey = e.getKey();
			}
		}
		if (maxVotes > 1) {
			return maxKey;
		}

		return null;
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
	}

	public static void unZip(InputStream zippedIS, File outputFolder)
			throws IOException {
		byte[] buffer = new byte[1024];

		ZipInputStream zis = new ZipInputStream(zippedIS);
		ZipEntry ze = zis.getNextEntry();

		while (ze != null) {
			String fileName = ze.getName();
			File newFile = new File(outputFolder + File.separator + fileName);
			new File(newFile.getParent()).mkdirs();
			FileOutputStream fos = new FileOutputStream(newFile);
			int len;
			while ((len = zis.read(buffer)) > 0) {
				fos.write(buffer, 0, len);
			}
			fos.close();
			ze = zis.getNextEntry();
		}

		zis.closeEntry();
		zis.close();
	}

	public static void deleteFolder(File folder) {
		File[] files = folder.listFiles();
		if (files != null) { // some JVMs return null for empty dirs
			for (File f : files) {
				if (f.isDirectory()) {
					deleteFolder(f);
				} else {
					f.delete();
				}
			}
		}
		folder.delete();
	}

}