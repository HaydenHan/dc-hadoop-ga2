package com.dataeye.hadoop.domain.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.AccountBaseWritable;
import com.dataeye.hadoop.domain.kv.AccountRollInfo;

public class AccountBackInfo extends AccountBaseWritable {

	private int isEverPay;
	private int nDayBack;
	
	public int getIsEverPay() {
		return isEverPay;
	}

	public void setIsEverPay(int isEverPay) {
		this.isEverPay = isEverPay;
	}

	public int getnDayBack() {
		return nDayBack;
	}

	public void setnDayBack(int nDayBack) {
		this.nDayBack = nDayBack;
	}

	public AccountBackInfo() {
	}

	public AccountBackInfo(AccountRollInfo info) {
		super(info);
	}
	
	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(isEverPay);
		out.writeInt(nDayBack);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.isEverPay = in.readInt();
		this.nDayBack = in.readInt();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsString(out);
		super.writeSeparator(out);
		super.writeUTF8WithSep(out, isEverPay);
		super.writeUTF8(out, nDayBack);
	}

}
