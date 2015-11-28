package com.dataeye.hadoop.common.dewritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class DEDynamicWritable extends DEWritable{
	
	public abstract int compareTo(DEDynamicWritable o);
	public abstract void write(DataOutput out) throws IOException;
	public abstract void readFields(DataInput in) throws IOException;
}