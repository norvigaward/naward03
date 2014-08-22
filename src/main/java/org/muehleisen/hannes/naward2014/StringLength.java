package org.muehleisen.hannes.naward2014;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class StringLength extends EvalFunc<Integer> {

	public Integer exec(Tuple input) throws IOException {
		if (input == null || input.size() < 1)
			return 0;
		try {
			return ((String) input.get(0)).length();
		} catch (Exception e) {
			warn(e.getMessage(), PigWarning.UDF_WARNING_5);
		}
		return 0;
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.INTEGER));
	}
}
