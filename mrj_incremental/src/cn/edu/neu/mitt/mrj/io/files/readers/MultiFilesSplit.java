package cn.edu.neu.mitt.mrj.io.files.readers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class MultiFilesSplit extends InputSplit implements Writable {

	private long totalLength = 0;
	private List<FileStatus> list = new ArrayList<FileStatus>();
	
	@Override
	public long getLength() {
		return totalLength;
	}
	
	public void addFile(FileStatus file) {
		list.add(file);
		totalLength += file.getLen();
	}
	
	public List<FileStatus> getFiles() {
		return list;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[]{};
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		totalLength = in.readLong();
		int numFiles = in.readInt();
		list.clear();
		for(int i = 0; i < numFiles; ++i) {
			FileStatus file = new FileStatus();
			file.readFields(in);
			list.add(file);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(totalLength);
		out.writeInt(list.size());
		for(FileStatus file : list) {
			file.write(out);
		}
	}

}
