package org.muehleisen.hannes.naward2014;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;

import uk.bl.wa.hadoop.WritableArchiveRecord;

public class ArchiveFileRecordReader extends
		RecordReader<Text, WritableArchiveRecord> {

	private FSDataInputStream datainputstream;
	private FileStatus status;
	private FileSystem filesystem;
	private Path[] paths;
	int currentPath = -1;
	Long offset = 0L;
	private ArchiveReader arcreader;
	private Iterator<ArchiveRecord> iterator;
	private ArchiveRecord record;
	private String archiveName;

	private Text key;
	private WritableArchiveRecord value;

	@Override
	public void close() throws IOException {
		if (datainputstream != null) {
			datainputstream.close();
		}
	}

	@Override
	public float getProgress() throws IOException {
		return datainputstream.getPos() / (1024 * 1024 * this.status.getLen());
	}

	private boolean nextFile() throws IOException {
		currentPath++;
		if (currentPath >= paths.length) {
			return false;
		}
		// Set up the ArchiveReader:
		this.status = this.filesystem.getFileStatus(paths[currentPath]);
		datainputstream = this.filesystem.open(paths[currentPath]);
		arcreader = (ArchiveReader) ArchiveReaderFactory.get(
				paths[currentPath].getName(), datainputstream, true);
		// Set to strict reading, in order to cope with malformed archive files
		// which cause an infinite loop otherwise.
		arcreader.setStrict(true);
		// Get the iterator:
		iterator = arcreader.iterator();
		this.archiveName = paths[currentPath].getName();
		return true;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public WritableArchiveRecord getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		if (split instanceof FileSplit) {
			this.paths = new Path[1];
			this.paths[0] = ((FileSplit) split).getPath();
		} else {
			throw new IOException("InputSplit is not a file split - aborting");
		}

		// get correct file system in case there are many (such as in EMR)
		this.filesystem = FileSystem.get(this.paths[0].toUri(),
				context.getConfiguration());
		for (Path p : this.paths) {
			System.out.println("Processing path: " + p);
		}
		// Queue up the iterator:
		this.nextFile();

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		key = new Text();
		value = new WritableArchiveRecord();

		boolean found = false;
		while (!found) {
			boolean hasNext = false;
			try {
				hasNext = iterator.hasNext();
			} catch (Throwable e) {
				hasNext = false;
			}
			try {
				if (hasNext) {
					record = (ArchiveRecord) iterator.next();
					found = true;
					key.set(this.archiveName);
					value.setRecord(record);
				} else if (!this.nextFile()) {
					break;
				}
			} catch (Throwable e) {
				found = false;
			}
		}
		return found;
	}
}