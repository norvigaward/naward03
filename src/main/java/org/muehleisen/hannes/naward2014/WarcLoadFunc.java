package org.muehleisen.hannes.naward2014;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.jsoup.Jsoup;
import org.jsoup.examples.HtmlToPlainText;


public class WarcLoadFunc extends FileInputLoadFunc implements LoadMetadata {
	private ArchiveFileRecordReader reader = null;

	@Override
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, location);
	}

	// oh the joys
	@SuppressWarnings({ "rawtypes" })
	@Override
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {
		// do nothing?
		this.reader = (ArchiveFileRecordReader) reader;
	}

	// some Hannes-TM HTTP header parsing kludges, way faster than libs
	private static String headerValue(String[] headers, String key, String dflt) {
		for (String hdrLine : headers) {
			if (hdrLine.toLowerCase().trim().startsWith(key.toLowerCase())) {
				return hdrLine.trim();
			}
		}
		return dflt;
	}

	private static String headerKeyValue(String[] headers, String key,
			String dflt) {
		String line = headerValue(headers, key, null);
		if (line == null)
			return dflt;
		String[] pces = line.split(":");
		if (pces.length != 2)
			return dflt;
		return pces[1].trim();
	}

	@Override
	public Tuple getNext() throws IOException {
		ArchiveRecordHeader header = null;
		ArchiveRecord record = null;
		String contentType = null;
		String headers[] = null;
		do {
			try {
				if (!this.reader.nextKeyValue()) {
					return null;
				}
				WritableArchiveRecord value = this.reader.getCurrentValue();
				record = value.getRecord();
			} catch (InterruptedException e) {
				continue;
			}
			if (record == null) {
				continue;
			}
			header = record.getHeader();
			headers = WARCRecordUtils.getHeaders(record, true).split("\n");
			if (headers.length < 1
					|| headerValue(headers, "HTTP/", null) == null) {
				continue;
			}
			contentType = headerKeyValue(headers, "Content-Type", "text/html");
			if (!contentType.contains("html")) {
				continue;
			}
		} while (!header.getMimetype().equals(
				"application/http; msgtype=response"));

		Charset cs = Charset.defaultCharset();
		try {
			String[] ctPces = contentType.split(";");
			if (ctPces.length == 2) {
				String csName = ctPces[1].toLowerCase().replace("charset=", "")
						.replace("\"", "").trim();
				cs = Charset.forName(csName);
			}
		} catch (Exception e) {
			// http://homepages.cwi.nl/~hannes/whatever.gif
		}

		// read warc payload with correct encoding
		String content = IOUtils.toString(new InputStreamReader(WARCRecordUtils
				.getPayload(record), cs));

		Tuple t = TupleFactory.getInstance().newTuple(7);
		t.set(0, header.getUrl());
		t.set(1, header.getHeaderValue("WARC-IP-Address"));
		t.set(2, header.getHeaderValue("WARC-Record-ID"));
		t.set(3, header.getLength());
		t.set(4, StringUtils.join(headers, "\n"));
		t.set(5, content);
		t.set(6, new HtmlToPlainText().getPlainText(Jsoup.parse(content)));
		return t;

	}

	@SuppressWarnings("rawtypes")
	@Override
	public InputFormat getInputFormat() throws IOException {
		// TODO Auto-generated method stub
		return new FileInputFormat<Text, WritableArchiveRecord>() {
			@Override
			public RecordReader<Text, WritableArchiveRecord> createRecordReader(
					InputSplit arg0, TaskAttemptContext arg1)
					throws IOException, InterruptedException {
				return new ArchiveFileRecordReader();
			}

			@Override
			protected boolean isSplitable(JobContext context, Path file) {
				return false;
			}
		};
	}

	public ResourceSchema getSchema(String location, Job job)
			throws IOException {
		List<ResourceFieldSchema> fields = new ArrayList<ResourceFieldSchema>();
		ResourceFieldSchema rs = new ResourceFieldSchema();
		rs.setName("url");
		rs.setType(DataType.CHARARRAY);
		fields.add(rs);
		rs = new ResourceFieldSchema();
		rs.setName("ip");
		rs.setType(DataType.CHARARRAY);
		fields.add(rs);
		rs = new ResourceFieldSchema();
		rs.setName("recordid");
		rs.setType(DataType.CHARARRAY);
		fields.add(rs);
		rs = new ResourceFieldSchema();
		rs.setName("length");
		rs.setType(DataType.LONG);
		fields.add(rs);
		rs = new ResourceFieldSchema();
		rs.setName("headers");
		rs.setType(DataType.CHARARRAY);
		fields.add(rs);
		rs = new ResourceFieldSchema();
		rs.setName("html");
		rs.setType(DataType.CHARARRAY);
		fields.add(rs);
		rs = new ResourceFieldSchema();
		rs.setName("plaintext");
		rs.setType(DataType.CHARARRAY);
		fields.add(rs);
		ResourceSchema schema = new ResourceSchema();
		schema.setFields(fields.toArray(new ResourceFieldSchema[0]));
		return schema;
	}

	public ResourceStatistics getStatistics(String location, Job job)
			throws IOException {
		return null;
	}

	public String[] getPartitionKeys(String location, Job job)
			throws IOException {
		return null;
	}

	public void setPartitionFilter(Expression partitionFilter)
			throws IOException {
	}
}
