package org.muehleisen.hannes.naward2014;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class Pr0nTagFinder extends EvalFunc<Boolean> {
	private static final Pattern ratingRegex = Pattern.compile(
			"RTA-5042-1996-1400-1577-RTA", Pattern.CASE_INSENSITIVE);

	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() < 1)
			return false;
		try {
			return ratingRegex.matcher((String) input.get(0)).find();
		} catch (Exception e) {
			warn(e.getMessage(), PigWarning.UDF_WARNING_1);
		}
		return false;
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.BOOLEAN));
	}
}