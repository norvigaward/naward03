package org.muehleisen.hannes.naward2014;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.mahout.classifier.naivebayes.BayesUtils;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.vectorizer.TFIDF;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;

public class ApplyClassifier extends EvalFunc<String> {

	Map<Integer, String> labels = null;
	Map<String, Integer> dictionary = null;
	Map<Integer, Long> documentFrequency = null;
	@SuppressWarnings("deprecation")
	Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_43);
	NaiveBayesModel model = null;

	public static Map<String, Integer> readDictionnary(Configuration conf,
			Path dictionnaryPath) {
		Map<String, Integer> dictionnary = new HashMap<String, Integer>();
		for (Pair<Text, IntWritable> pair : new SequenceFileIterable<Text, IntWritable>(
				dictionnaryPath, true, conf)) {
			dictionnary.put(pair.getFirst().toString(), pair.getSecond().get());
		}
		return dictionnary;
	}

	public static Map<Integer, Long> readDocumentFrequency(Configuration conf,
			Path documentFrequencyPath) {
		Map<Integer, Long> documentFrequency = new HashMap<Integer, Long>();
		PathFilter filter = new PathFilter() {

			public boolean accept(Path arg0) {
				return !arg0.toString().contains("_SUCCESS");
			}
		};
		for (Pair<IntWritable, LongWritable> pair : new SequenceFileDirIterable<IntWritable, LongWritable>(
				documentFrequencyPath, PathType.LIST, filter, conf)) {
			documentFrequency
					.put(pair.getFirst().get(), pair.getSecond().get());
		}
		return documentFrequency;
	}

	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() < 4)
			return null;
		try {

			String contentPlain = (String) input.get(0);
			String modelPath = (String) input.get(1);
			String labelIndexPath = (String) input.get(2);
			String dictionaryPath = (String) input.get(3);
			String documentFrequencyPath = (String) input.get(4);

			if (labels == null) {
				Configuration c = UDFContext.getUDFContext().getJobConf();
				model = NaiveBayesModel.materialize(new Path(modelPath), c);
				labels = BayesUtils.readLabelIndex(c, new Path(labelIndexPath));
				dictionary = readDictionnary(c, new Path(dictionaryPath));
				documentFrequency = readDocumentFrequency(c, new Path(
						documentFrequencyPath));
			}
			Multiset<String> words = ConcurrentHashMultiset.create();
			int documentCount = documentFrequency.get(-1).intValue();
			// model is a matrix (wordId, labelId) => probability score

			StandardNaiveBayesClassifier classifier = new StandardNaiveBayesClassifier(
					model);

			TokenStream ts = analyzer.tokenStream("text", new StringReader(
					contentPlain));
			CharTermAttribute termAtt = ts
					.addAttribute(CharTermAttribute.class);
			ts.reset();
			int wordCount = 0;
			while (ts.incrementToken()) {
				if (termAtt.length() > 0) {
					String word = ts.getAttribute(CharTermAttribute.class)
							.toString();
					Integer wordId = dictionary.get(word);
					// if the word is not in the dictionary, skip it
					if (wordId != null) {
						words.add(word);
						wordCount++;
					}
				}
			}
			ts.close();

			// create vector wordId => weight using tfidf
			Vector vector = new RandomAccessSparseVector(10000);
			TFIDF tfidf = new TFIDF();
			for (Multiset.Entry<String> entry : words.entrySet()) {
				String word = entry.getElement();
				int count = entry.getCount();
				Integer wordId = dictionary.get(word);
				Long freq = documentFrequency.get(wordId);
				double tfIdfValue = tfidf.calculate(count, freq.intValue(),
						wordCount, documentCount);
				vector.setQuick(wordId, tfIdfValue);
			}
			// With the classifier, we get one score for each label
			// The label with the highest score is the one the tweet is more
			// likely to
			// be associated to
			Vector resultVector = classifier.classifyFull(vector);
			double bestScore = -Double.MAX_VALUE;
			int bestCategoryId = -1;
			for (Element element : resultVector.all()) {
				int categoryId = element.index();
				double score = element.get();
				if (score > bestScore) {
					bestScore = score;
					bestCategoryId = categoryId;
				}
			}
			return labels.get(bestCategoryId);

		} catch (Exception e) {
			warn(e.getMessage(), PigWarning.UDF_WARNING_1);
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
	}
}