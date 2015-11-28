package com.dataeye.hadoop.domain.result.role;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.RoleBaseWritable;
import com.dataeye.hadoop.domain.hbase.RoleHistoryInfo;
import com.dataeye.hadoop.domain.kv.role.RoleRollInfo;

public class RoleLostInfo extends RoleBaseWritable {
	private int isEverPay;
	private int lastLoginDate;
	private int nDayLost;
	private int totalOnlineDays;
	private int totalOnlineTime;
	private int totalLoginTimes;

	public int getIsEverPay() {
		return isEverPay;
	}

	public void setIsEverPay(int isEverPay) {
		this.isEverPay = isEverPay;
	}

	public int getLastLoginDate() {
		return lastLoginDate;
	}

	public void setLastLoginDate(int lastLoginDate) {
		this.lastLoginDate = lastLoginDate;
	}

	public int getnDayLost() {
		return nDayLost;
	}

	public void setnDayLost(int nDayLost) {
		this.nDayLost = nDayLost;
	}

	public int getTotalOnlineDays() {
		return totalOnlineDays;
	}

	public void setTotalOnlineDays(int totalOnlineDays) {
		this.totalOnlineDays = totalOnlineDays;
	}

	public int getTotalOnlineTime() {
		return totalOnlineTime;
	}

	public void setTotalOnlineTime(int totalOnlineTime) {
		this.totalOnlineTime = totalOnlineTime;
	}

	public int getTotalLoginTimes() {
		return totalLoginTimes;
	}

	public void setTotalLoginTimes(int totalLoginTimes) {
		this.totalLoginTimes = totalLoginTimes;
	}

	public RoleLostInfo() {

	}

	public RoleLostInfo(RoleRollInfo rollInfo) {
		super(rollInfo);
		this.lastLoginDate = rollInfo.getLastLoginDate();
		this.totalOnlineDays = rollInfo.getTotalOnlineDays();
		this.totalOnlineTime = rollInfo.getTotalOnlineTime();
		this.totalLoginTimes = rollInfo.getTotalLoginTimes();
	}

	public RoleLostInfo(RoleHistoryInfo roleHistry) {
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
		out.writeInt(lastLoginDate);
		out.writeInt(nDayLost);
		out.writeInt(totalOnlineDays);
		out.writeInt(totalOnlineTime);
		out.writeInt(totalLoginTimes);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.isEverPay = in.readInt();
		this.lastLoginDate = in.readInt();
		this.nDayLost = in.readInt();
		this.totalOnlineDays = in.readInt();
		this.totalOnlineTime = in.readInt();
		this.totalLoginTimes = in.readInt();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsString(out);
		super.writeSeparator(out);
		super.writeUTF8WithSep(out, this.isEverPay);
		super.writeUTF8WithSep(out, this.lastLoginDate);
		super.writeUTF8WithSep(out, this.nDayLost);
		super.writeUTF8WithSep(out, this.totalOnlineDays);
		super.writeUTF8WithSep(out, this.totalOnlineTime);
		super.writeUTF8(out, this.totalLoginTimes);
	}
}
