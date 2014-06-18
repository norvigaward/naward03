#!/bin/sh
set -x
cp target/naward2014-0.0.1-SNAPSHOT.jar lib
export MAHOUT_LOCAL=true 
export PIG_HEAPSIZE=14000
OUTDIR=/export/scratch2/hannes/warcs/out
INPUT=/export/scratch2/hannes/warcs/dataset
#INPUT=/export/scratch2/hannes/warcs/IAH-20080430204825-00000-blackbook.warc.gz

rm -r $OUTDIR
mkdir -p $OUTDIR

# convert data from WARC to SequenceFile to make Mahout happy
pig -x local -p LIBS=/export/scratch2/hannes/naward03/lib -p INPUT=$INPUT -p OUTPUT=$OUTDIR \
src/main/pig/prepare-data.pig
mahout seq2sparse -i $OUTDIR/trainingset -o $OUTDIR/trainingset-seq

# split training set
mahout split -i $OUTDIR/trainingset-seq/tfidf-vectors \
--trainingOutput $OUTDIR/train-vectors --testOutput $OUTDIR/test-vectors \
--randomSelectionPct 40 --overwrite --sequenceFiles -xm sequential

# actually train classifier
mahout trainnb -i $OUTDIR/train-vectors -el -li $OUTDIR/labelindex -o $OUTDIR/model -ow -c

# apply classifier
mahout testnb -i $OUTDIR/test-vectors -m $OUTDIR/model -l $OUTDIR/labelindex -ow -o $OUTDIR/testing-results -ow -c

# feed results back into pig for further aggregation
pig -x local -p LIBS=/export/scratch2/hannes/naward03/lib -p INPUT=$OUTDIR -p OUTPUT=$OUTDIR \
src/main/pig/combine-data.pig

hadoop fs -text $OUTDIR/mapinput > $OUTDIR/map.tsv