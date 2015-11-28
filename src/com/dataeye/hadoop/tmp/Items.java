package com.dataeye.hadoop.tmp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.hadoop.io.WritableComparable;

public class Items extends DCWritable implements WritableComparable<DCWritable> {

	private int counts;
	private int money;

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

	public Items() {
	}

	public Items(int counts, int money) {
		this.counts = counts;
		this.money = money;
	}

	public Items(String[] fields) {
		int i = 0;
		this.counts = Integer.valueOf(fields[i++]);
		this.money = Integer.valueOf(fields[i++]);
	}
	
	@Override
	public int compareTo(DCWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.counts = in.readInt();
		this.money = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(counts);
		out.writeInt(money);
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsUTF8Bytes(out, counts);
		super.writeSeparator(out);
		super.writeAsUTF8Bytes(out, money);
	}
}
