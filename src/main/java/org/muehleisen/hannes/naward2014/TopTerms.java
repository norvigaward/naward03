package org.muehleisen.hannes.naward2014;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.classifier.naivebayes.BayesUtils;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;

public class TopTerms {

	public static Map<Integer, String> readInverseDictionnary(
			Configuration conf, Path dictionnaryPath) {
		Map<Integer, String> inverseDictionnary = new HashMap<Integer, String>();
		for (Pair<Text, IntWritable> pair : new SequenceFileIterable<Text, IntWritable>(
				dictionnaryPath, true, conf)) {
			inverseDictionnary.put(pair.getSecond().get(), pair.getFirst()
					.toString());
		}
		return inverseDictionnary;
	}

	public static Map<Integer, Long> getTopWords(
			Map<Integer, Long> documentFrequency, int topWordsCount) {
		List<Map.Entry<Integer, Long>> entries = new ArrayList<Map.Entry<Integer, Long>>(
				documentFrequency.entrySet());
		Collections.sort(entries, new Comparator<Map.Entry<Integer, Long>>() {
			public int compare(Entry<Integer, Long> e1, Entry<Integer, Long> e2) {
				return -e1.getValue().compareTo(e2.getValue());
			}
		});

		Map<Integer, Long> topWords = new HashMap<Integer, Long>();
		int i = 0;
		for (Map.Entry<Integer, Long> entry : entries) {
			topWords.put(entry.getKey(), entry.getValue());
			i++;
			if (i > topWordsCount) {
				break;
			}
		}

		return topWords;
	}

	public static class WordWeight implements Comparable<WordWeight> {
		private int wordId;
		private double weight;

		public WordWeight(int wordId, double weight) {
			this.wordId = wordId;
			this.weight = weight;
		}

		public int getWordId() {
			return wordId;
		}

		public Double getWeight() {
			return weight;
		}

		public int compareTo(WordWeight w) {
			return -getWeight().compareTo(w.getWeight());
		}

	}

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.out
					.println("Arguments: [model] [label index] [dictionnary] [document frequency]");
			return;
		}
		String modelPath = args[0];
		String labelIndexPath = args[1];
		String dictionaryPath = args[2];
		String documentFrequencyPath = args[3];

		Configuration configuration = new Configuration();

		// model is a matrix (wordId, labelId) => probability score
		NaiveBayesModel model = NaiveBayesModel.materialize(
				new Path(modelPath), configuration);

		// labels is a map label => classId
		Map<Integer, String> labels = BayesUtils.readLabelIndex(configuration,
				new Path(labelIndexPath));
		Map<Integer, String> inverseDictionary = readInverseDictionnary(
				configuration, new Path(dictionaryPath));
		Map<Integer, Long> documentFrequency = NaiveBayesClassifier
				.readDocumentFrequency(configuration, new Path(
						documentFrequencyPath));

		Map<Integer, Long> topWords = getTopWords(documentFrequency, 50);
		for (Map.Entry<Integer, Long> entry : topWords.entrySet()) {
		//	System.out.println(inverseDictionary.get(entry.getKey())	+ "\t" + Math.round(entry.getValue()));
		}

		for (int labelId = 0; labelId < model.numLabels(); labelId++) {
			if (!labels.get(labelId).equals("porn")) {
				continue;
			}
			SortedSet<WordWeight> wordWeights = new TreeSet<WordWeight>();
			for (int wordId = 0; wordId < model.numFeatures(); wordId++) {
				WordWeight w = new WordWeight(wordId, model.weight(labelId,
						wordId));
				wordWeights.add(w);
			}
			int i = 0;

			for (WordWeight w : wordWeights) {
				System.out.println(inverseDictionary.get(w.getWordId())	+ "\t" + Math.round(w.getWeight()));
				i++;
				if (i >= 500) {
					break;
				}
			}
			System.out.println();
		}
	}
}
