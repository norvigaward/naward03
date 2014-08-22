REGISTER '$LIBS/naward2014-0.0.1-SNAPSHOT.jar'; 

SET default_parallel 40;
SET mapreduce.map.failures.maxpercent 10;

%declare minwc 100

raw = LOAD '$INPUT' 
USING org.muehleisen.hannes.naward2014.WarcLoadFunc()
AS (url:chararray, ip:chararray, recordid:chararray, length:long, headers:chararray, content:chararray, plaintext:chararray);

--store raw into '/tmp/demoout' using BinStorage();

--trainingset = SAMPLE raw $learnset_size; 
--validationset = SAMPLE raw $learnset_size; 
--trainingset = LIMIT raw 100; 
trainingset = raw;
--validationset = LIMIT raw $learnset_size*2;

trainingset_annot = FOREACH trainingset GENERATE 
 (org.muehleisen.hannes.naward2014.Pr0nTagFinder(content) or 
 	org.muehleisen.hannes.naward2014.BlacklistDomainFinder(url) or 
 	org.muehleisen.hannes.naward2014.USC2257Finder(plaintext) ? 'porn':'clean') as class:chararray,plaintext;

tc = FOREACH (GROUP trainingset_annot ALL) GENERATE COUNT(trainingset_annot) as totalcount;
--STORE tc into '$OUTPUT/tscount' using PigStorage('\t');

-- priors
by_class = GROUP trainingset_annot BY class;
by_class_doc_counts = FOREACH by_class GENERATE
    FLATTEN(group) AS class2, COUNT(trainingset_annot) as doccount;
--STORE by_class_doc_counts into '$OUTPUT/classcount' using PigStorage('\t');

-- explode documents to word lists
trainwords = FOREACH trainingset_annot GENERATE class,FLATTEN(org.muehleisen.hannes.naward2014.SplitBag(LOWER(plaintext),' ',10000)) as word; -- this is the max number of words on a page

-- document frequencies for later
by_word = GROUP trainwords by word;
by_word_counts = FOREACH by_word GENERATE FLATTEN(group) AS word2, COUNT(trainwords) as df;
by_word_counts_r = FILTER by_word_counts BY df > $minwc;

-- word probabilities
by_word_class = GROUP trainwords BY (class,word);
by_word_class_counts = FOREACH by_word_class GENERATE
    FLATTEN(group) AS (class, word),
    COUNT(trainwords) AS wordcount;


-- filter low-frequency and long words
by_word_class_counts_rel = FILTER by_word_class_counts BY wordcount > $minwc and org.muehleisen.hannes.naward2014.StringLength(word) > 2 and org.muehleisen.hannes.naward2014.StringLength(word) < 20 and 
word matches '\\w+';

-- number of words in class
by_class_words = GROUP by_word_class_counts_rel by class;
by_class_word_counts = FOREACH by_class_words GENERATE FLATTEN(group) AS class3, SUM(by_word_class_counts_rel.wordcount) as wordsclass;


by_word_class_counts_p = JOIN by_word_class_counts_rel BY class, by_class_doc_counts BY class2, by_class_word_counts BY class3 USING 'replicated'; -- class2 we only join by this
by_word_class_counts_df = JOIN by_word_counts_r BY word2, by_word_class_counts_p BY word USING 'replicated';
by_word_class_counts_c = FOREACH by_word_class_counts_df GENERATE word,class2,wordcount,wordsclass,doccount,(doccount*1.0)/tc.totalcount,tc.totalcount,(wordcount*1.0)/wordsclass;

STORE by_word_class_counts_c into '$OUTPUT/vocab' using PigStorage('\t');

--DUMP by_word_class_counts_c;
