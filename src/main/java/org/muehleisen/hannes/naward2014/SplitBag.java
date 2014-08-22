package org.muehleisen.hannes.naward2014;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class SplitBag extends EvalFunc<DataBag> {

	public DataBag exec(Tuple input) throws IOException {
		DataBag output = BagFactory.getInstance().newDefaultBag();
		if (input == null || input.size() < 3)
			return output;
		try {
			String[] results = ((String) input.get(0)).split((String) input
					.get(1));
			int limit = (Integer) input.get(2);
			TupleFactory mTupleFactory = TupleFactory.getInstance();
			for (int l = 0; l < Math.min(results.length, limit); l++) {
				if (results[l] == null || "".equals(results[l])) {
					continue;
				}
				output.add(mTupleFactory.newTuple(results[l]));
			}
		} catch (Exception e) {
			warn(e.getMessage(), PigWarning.UDF_WARNING_4);
			e.printStackTrace();
		}
		return output;
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.BAG));
	}
}