package org.muehleisen.hannes.naward2014;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.jsoup.Jsoup;
import org.jsoup.examples.HtmlToPlainText;
import org.terrier.indexing.TaggedDocument;
import org.terrier.indexing.tokenisation.EnglishTokeniser;
import org.terrier.terms.PorterStemmer;

public class TokenizeStemStopfilter extends EvalFunc<Tuple> {

	private final static TupleFactory tupleFactory = TupleFactory.getInstance();
	private PorterStemmer stemmer = new PorterStemmer();
	private final static Set<String> stopwords = new HashSet<String>();
	static {
		// first load stopword list (lifted from Terrier 3.5.0)
		BufferedReader rdr = new BufferedReader(new InputStreamReader(
				TokenizeStemStopfilter.class.getClassLoader()
						.getResourceAsStream("stopword-list.txt")));
		String sw = null;
		try {
			while ((sw = rdr.readLine()) != null) {
				stopwords.add(sw.trim());
			}
		} catch (IOException e) {
		}
	}

	@Override
	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() < 1)
			return null;
		try {
			String content = (String) input.get(0);

			String contentPlain = new HtmlToPlainText().getPlainText(Jsoup
					.parse(content));

			// run terrier tokenizer etc
			TaggedDocument d = new TaggedDocument(
					new StringReader(contentPlain), null,
					new EnglishTokeniser());

			List<String> terms = new LinkedList<String>();
			while (!d.endOfDocument()) {
				String term = d.getNextTerm();
				if (term == null) {
					continue;
				}
				if (term.length() < 3) {
					continue;
				}
				term = stemmer.stem(term);
				if (stopwords.contains(term)) {
					continue;
				}
				terms.add(term);
			}
			return tupleFactory.newTuple(terms);

		} catch (Exception e) {
			warn(e.getMessage(), PigWarning.UDF_WARNING_1);
		}
		return null;
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.TUPLE));
	}
}