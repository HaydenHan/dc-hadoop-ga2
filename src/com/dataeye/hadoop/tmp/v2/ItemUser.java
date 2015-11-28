package com.dataeye.hadoop.tmp.v2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

public class ItemUser extends DCWritable {

	private String username;
	private int count;
	private int money;
	
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public int getMoney() {
		return money;
	}
	public void setMoney(int money) {
		this.money = money;
	}
	
	public ItemUser() {
	}
	
	@Override
	public int compareTo(DCWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(username);
		out.writeInt(count);
		out.writeInt(money);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.username = in.readUTF();
		this.count = in.readInt();
		this.money = in.readInt();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsUTF8Bytes(out, username);
		super.writeSeparator(out);
		super.writeAsUTF8Bytes(out, count);
		super.writeSeparator(out);
		super.writeAsUTF8Bytes(out, money);
	}
}
