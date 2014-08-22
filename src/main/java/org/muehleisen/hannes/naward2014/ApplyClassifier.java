package org.muehleisen.hannes.naward2014;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class ApplyClassifier extends EvalFunc<String> {

	private static final List<String> classes = Arrays.asList("clean", "porn");
	private static final Map<String, Map<String, Double>> weights = new HashMap<String, Map<String, Double>>();
	private static final Map<String, Double> priors = new HashMap<String, Double>();
	private static final Set<String> stopwords = new HashSet<String>();
	
	static {
		for (String cl : classes) {
			weights.put(cl, new HashMap<String, Double>());
		}
		try {
			// load vocab
			BufferedReader rdr = new BufferedReader(new InputStreamReader(
					new GZIPInputStream(ApplyClassifier.class.getClassLoader()
							.getResourceAsStream("vocab.tsv.gz"))));
			String sw = null;
			while ((sw = rdr.readLine()) != null) {
				String[] ln = sw.split("\t");
				String w = ln[0];
				String c = ln[1];
				priors.put(c, Double.parseDouble(ln[5]));
				weights.get(c).put(w, Double.parseDouble(ln[7]));
			}
			
			// load stopwords
			rdr = new BufferedReader(new InputStreamReader(
					ApplyClassifier.class.getClassLoader()
							.getResourceAsStream("stopword-list.txt")));
			sw = null;
			while ((sw = rdr.readLine()) != null) {
				sw =sw.toLowerCase().trim();
				if (sw.length() > 0) {
					stopwords.add(sw);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() < 1)
			return null;
		Map<String, Double> cp = new HashMap<String, Double>();
		for (String cl : classes) {
			cp.put(cl, 0.0);
		}
		try {
			String[] words = ((String) input.get(0)).toLowerCase().split(" ");
			for (String word : words) {
				word = word.trim();
				if (word.length() < 1) {
					continue;
				}
				if (stopwords.contains(word)) {
					continue;
				}
				for (String cl : classes) {
					if (weights.get(cl).containsKey(word)) {
						//System.out.println(word+": "+weights.get(cl).get(word));
						cp.put(cl, cp.get(cl) + weights.get(cl).get(word));
					}
				}
			}
			//System.out.println(cp);
			// now decide
			double maxWt = Double.NEGATIVE_INFINITY;
			String maxCl = null;
			for (String cl : classes) {
				double p = priors.get(cl) * cp.get(cl);
				
				if (p > maxWt) {
					maxWt = p;
					maxCl = cl;
				}
			}
			return maxCl;
		} catch (Exception e) {
			e.printStackTrace();
			warn(e.getMessage(), PigWarning.UDF_WARNING_1);
		}
		return null;
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
	}

}
