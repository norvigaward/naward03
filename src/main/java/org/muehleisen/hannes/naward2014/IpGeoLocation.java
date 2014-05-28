package org.muehleisen.hannes.naward2014;

import java.io.IOException;
import java.net.InetAddress;
import java.util.zip.GZIPInputStream;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CountryResponse;

public class IpGeoLocation extends EvalFunc<String> {

	private static DatabaseReader db;

	static {
		try {
			db = new DatabaseReader.Builder(new GZIPInputStream(
					TokenizeStemStopfilter.class.getClassLoader()
							.getResourceAsStream("GeoLite2-Country.mmdb.gz")))
					.build();
		} catch (IOException e) {
		}
	}

	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() < 1)
			return null;
		try {
			CountryResponse cres = db.country(InetAddress
					.getByName(((String) input.get(0))));
			return cres.getCountry().getIsoCode();
		} catch (Exception e) {
			warn(e.getMessage(), PigWarning.UDF_WARNING_1);
		}
		return null;
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
	}
}