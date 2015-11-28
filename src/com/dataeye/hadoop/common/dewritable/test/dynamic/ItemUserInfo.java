package com.dataeye.hadoop.common.dewritable.test.dynamic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;

public class ItemUserInfo extends DEWritable {

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
	
	public ItemUserInfo() {
	}
	
	@Override
	public int compareTo(DEWritable o) {
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
