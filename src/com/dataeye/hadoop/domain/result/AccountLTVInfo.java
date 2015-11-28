package com.dataeye.hadoop.domain.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.AccountBaseWritable;
import com.dataeye.hadoop.domain.hbase.AccountHistoryInfo;

public class AccountLTVInfo extends AccountBaseWritable {

	private int firstLoginDate;
	private int nDayLife;
	private float nDayValue;

	public int getFirstLoginDate() {
		return firstLoginDate;
	}

	public void setFirstLoginDate(int firstLoginDate) {
		this.firstLoginDate = firstLoginDate;
	}

	public int getnDayLife() {
		return nDayLife;
	}

	public void setnDayLife(int nDayLife) {
		this.nDayLife = nDayLife;
	}

	public float getnDayValue() {
		return nDayValue;
	}

	public void setnDayValue(float nDayValue) {
		this.nDayValue = nDayValue;
	}

	public AccountLTVInfo() {
	}

	public AccountLTVInfo(AccountHistoryInfo info) {
		super(info);
	}

	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(firstLoginDate);
		out.writeInt(nDayLife);
		out.writeFloat(nDayValue);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.firstLoginDate = in.readInt();
		this.nDayLife = in.readInt();
		this.nDayValue = in.readFloat();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsString(out);
		super.writeSeparator(out);
		super.writeUTF8WithSep(out, firstLoginDate);
		super.writeUTF8WithSep(out, nDayLife);
		super.writeUTF8(out, nDayValue);
	}
}
