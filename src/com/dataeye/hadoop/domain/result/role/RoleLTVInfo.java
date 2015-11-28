package com.dataeye.hadoop.domain.result.role;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.RoleBaseWritable;
import com.dataeye.hadoop.domain.hbase.RoleHistoryInfo;
import com.dataeye.hadoop.domain.kv.role.RoleRollInfo;

public class RoleLTVInfo extends RoleBaseWritable {
	private int isEverPay;
	private int firstLoginDate;
	private int nDayLife;
	private float nDayValue;

	public int getIsEverPay() {
		return isEverPay;
	}

	public void setIsEverPay(int isEverPay) {
		this.isEverPay = isEverPay;
	}

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

	public RoleLTVInfo() {

	}

	public RoleLTVInfo(RoleRollInfo rollInfo) {
		super(rollInfo);
	}

	public RoleLTVInfo(RoleHistoryInfo roleHistry) {
		super(roleHistry);
	}

	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(isEverPay);
		out.writeInt(firstLoginDate);
		out.writeInt(nDayLife);
		out.writeFloat(nDayValue);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.isEverPay = in.readInt();
		this.firstLoginDate = in.readInt();
		this.nDayLife = in.readInt();
		this.nDayValue = in.readFloat();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsString(out);
		super.writeSeparator(out);
		super.writeUTF8WithSep(out, this.isEverPay);
		super.writeUTF8WithSep(out, this.firstLoginDate);
		super.writeUTF8WithSep(out, this.nDayLife);
		super.writeUTF8(out, this.nDayValue);
	}
}
