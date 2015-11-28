package com.dataeye.hadoop.tmp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.hadoop.io.WritableComparable;

public class Person extends DCWritable implements WritableComparable<DCWritable> {

	private String name;
	private int age;
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}
	
	public Person() {
	}

	public Person(String name, int age) {
		this.name = name;
		this.age = age;
	}
	
	public Person(String[] fields) {
		int i = 0;
		this.name = fields[i++];
		this.age = Integer.valueOf(fields[i++]);
	}

	@Override
	public int compareTo(DCWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.age = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeInt(age);
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsUTF8Bytes(out, name);
		super.writeSeparator(out);
		super.writeAsUTF8Bytes(out, age);
	}
}
