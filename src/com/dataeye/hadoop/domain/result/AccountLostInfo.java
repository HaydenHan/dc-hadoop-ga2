package com.dataeye.hadoop.domain.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.AccountBaseWritable;
import com.dataeye.hadoop.domain.hbase.AccountHistoryInfo;

public class AccountLostInfo extends AccountBaseWritable {

	private int isEverPayPlayer;
	private int lastLogDate;
	private int nDayLost;

	// 总在线天数
	private int totalOnlineDays;
	// 总在线时长
	private int totalOnlineTime;
	// 总登录次数
	private int totalLoginTimes;

	public int getnDayLost() {
		return nDayLost;
	}

	public void setnDayLost(int nDayLost) {
		this.nDayLost = nDayLost;
	}

	public int getLastLogDate() {
		return lastLogDate;
	}

	public void setLastLogDate(int lastLogDate) {
		this.lastLogDate = lastLogDate;
	}

	public int getIsEverPayPlayer() {
		return isEverPayPlayer;
	}

	public void setIsEverPayPlayer(int isEverPayPlayer) {
		this.isEverPayPlayer = isEverPayPlayer;
	}

	public int getTotalOnlineDays() {
		return totalOnlineDays;
	}

	public void setTotalOnlineDays(int totalOnlineDays) {
		this.totalOnlineDays = totalOnlineDays;
	}

	public int getTotalLoginTimes() {
		return totalLoginTimes;
	}

	public void setTotalLoginTimes(int totalLoginTimes) {
		this.totalLoginTimes = totalLoginTimes;
	}

	public int getTotalOnlineTime() {
		return totalOnlineTime;
	}

	public void setTotalOnlineTime(int totalOnlineTime) {
		this.totalOnlineTime = totalOnlineTime;
	}

	public AccountLostInfo() {
	}

	public AccountLostInfo(AccountHistoryInfo histInfo) {
		super(histInfo);
	}

	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(this.isEverPayPlayer);
		out.writeInt(this.lastLogDate);
		out.writeInt(this.nDayLost);
		out.writeInt(this.totalOnlineDays);
		out.writeInt(this.totalLoginTimes);
		out.writeInt(this.totalOnlineTime);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.isEverPayPlayer = in.readInt();
		this.lastLogDate = in.readInt();
		this.nDayLost = in.readInt();
		this.totalOnlineDays = in.readInt();
		this.totalLoginTimes = in.readInt();
		this.totalOnlineTime = in.readInt();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsString(out);
		super.writeSeparator(out);
		super.writeUTF8WithSep(out, isEverPayPlayer);
		super.writeUTF8WithSep(out, lastLogDate);
		super.writeUTF8WithSep(out, nDayLost);
		super.writeUTF8WithSep(out, totalOnlineDays);
		super.writeUTF8WithSep(out, totalLoginTimes);
		super.writeUTF8(out, totalOnlineTime);
	}
}
