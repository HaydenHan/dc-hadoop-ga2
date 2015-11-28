package com.dataeye.hadoop.common.dewritable.test.dynamic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;

public class ItemName extends DEWritable {

	private String name;
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public ItemName() {
	}

	public ItemName(String name) {
		this.name = name;
	}
	
	public ItemName(String[] fields) {
		int i = 0;
		this.name = fields[i++];
	}

	
	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsUTF8Bytes(out, name);
	}

	@Override
	public String toString() {
		return "ItemName [name=" + name + "]";
	}
}
