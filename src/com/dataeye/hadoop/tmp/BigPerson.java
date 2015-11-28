package com.dataeye.hadoop.tmp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

public class BigPerson extends Person {

	private String name;
	private int age;
	private int counts;
	private int money;
	
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

	public int getCounts() {
		return counts;
	}

	public void setCounts(int counts) {
		this.counts = counts;
	}

	public int getMoney() {
		return money;
	}

	public void setMoney(int money) {
		this.money = money;
	}

	public BigPerson(String name, int age, int counts, int money) {
		this.name = name;
		this.age = age;
		this.counts = counts;
		this.money = money;
	}

	@Override
	public int compareTo(DCWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.age = in.readInt();
		this.counts = in.readInt();
		this.money = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeInt(age);
		out.writeInt(counts);
		out.writeInt(money);
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsUTF8Bytes(out, name);
		super.writeSeparator(out);
		super.writeAsUTF8Bytes(out, age);
		super.writeSeparator(out);
		super.writeAsUTF8Bytes(out, counts);
		super.writeSeparator(out);
		super.writeAsUTF8Bytes(out, money);
	}
}
