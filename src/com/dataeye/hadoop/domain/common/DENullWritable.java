package com.dataeye.hadoop.domain.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.dataeye.hadoop.common.dewritable.DEWritable;

public class DENullWritable extends DEWritable {
	
	private static DENullWritable instance = new DENullWritable();;
	
	public static DENullWritable get(){
		return instance;
	}
	
	private DENullWritable() {
	}

	
	@Override
	public int compareTo(DEWritable o) {
		return 0;
	}

	@Override
	public void write(DataOutput out) throws IOException {
	}

	@Override
	public void readFields(DataInput in) throws IOException {
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
	}
}
