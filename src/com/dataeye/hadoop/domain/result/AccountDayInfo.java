package com.dataeye.hadoop.domain.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.AccountBaseWritable;
import com.dataeye.hadoop.domain.kv.AccountRollInfo;

public class AccountDayInfo extends AccountBaseWritable {

	private int isNewPlayer;
	private int isPayPlayer;
	private int isPayAtFirstDay;
	private int isEverPayPlayer;
	private int isNewAndCreateRole;

	private int dayLoginTimes;
	private int dayOnlineTime;
	private int dayPayTimes;
	private float dayPayAmount;

	// 总在线天数
	private int totalOnlineDays;
	// 总登录次数
	private int totalLoginTimes;
	// 总在线时长
	private int totalOnlineTime;
	// 总付费金额
	private float totalPayAmount;
	// 总付费次数
	private int totalPayTimes;

	public int getIsNewPlayer() {
		return isNewPlayer;
	}

	public void setIsNewPlayer(int isNewPlayer) {
		this.isNewPlayer = isNewPlayer;
	}

	public int getIsPayPlayer() {
		return isPayPlayer;
	}

	public void setIsPayPlayer(int isPayPlayer) {
		this.isPayPlayer = isPayPlayer;
	}

	public int getIsPayAtFirstDay() {
		return isPayAtFirstDay;
	}

	public void setIsPayAtFirstDay(int isPayAtFirstDay) {
		this.isPayAtFirstDay = isPayAtFirstDay;
	}

	public int getIsEverPayPlayer() {
		return isEverPayPlayer;
	}

	public void setIsEverPayPlayer(int isEverPayPlayer) {
		this.isEverPayPlayer = isEverPayPlayer;
	}

	public int getIsNewAndCreateRole() {
		return isNewAndCreateRole;
	}

	public void setIsNewAndCreateRole(int isNewAndCreateRole) {
		this.isNewAndCreateRole = isNewAndCreateRole;
	}

	public int getDayLoginTimes() {
		return dayLoginTimes;
	}

	public void setDayLoginTimes(int dayLoginTimes) {
		this.dayLoginTimes = dayLoginTimes;
	}

	public int getDayOnlineTime() {
		return dayOnlineTime;
	}

	public void setDayOnlineTime(int dayOnlineTime) {
		this.dayOnlineTime = dayOnlineTime;
	}

	public int getDayPayTimes() {
		return dayPayTimes;
	}

	public void setDayPayTimes(int dayPayTimes) {
		this.dayPayTimes = dayPayTimes;
	}

	public float getDayPayAmount() {
		return dayPayAmount;
	}

	public void setDayPayAmount(float dayPayAmount) {
		this.dayPayAmount = dayPayAmount;
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

	public float getTotalPayAmount() {
		return totalPayAmount;
	}

	public void setTotalPayAmount(float totalPayAmount) {
		this.totalPayAmount = totalPayAmount;
	}

	public int getTotalPayTimes() {
		return totalPayTimes;
	}

	public void setTotalPayTimes(int totalPayTimes) {
		this.totalPayTimes = totalPayTimes;
	}

	public AccountDayInfo() {
	}

	public AccountDayInfo(AccountRollInfo info) {
		super(info);
	}

	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(isNewPlayer);
		out.writeInt(isPayPlayer);
		out.writeInt(isPayAtFirstDay);
		out.writeInt(isEverPayPlayer);
		out.writeInt(isNewAndCreateRole);
		out.writeInt(dayLoginTimes);
		out.writeInt(dayOnlineTime);
		out.writeInt(dayPayTimes);
		out.writeFloat(dayPayAmount);
		out.writeInt(totalOnlineDays);
		out.writeInt(totalLoginTimes);
		out.writeInt(totalOnlineTime);
		out.writeFloat(totalPayAmount);
		out.writeInt(totalPayTimes);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.isNewPlayer = in.readInt();
		this.isPayPlayer = in.readInt();
		this.isPayAtFirstDay = in.readInt();
		this.isEverPayPlayer = in.readInt();
		this.isNewAndCreateRole = in.readInt();
		this.dayLoginTimes = in.readInt();
		this.dayOnlineTime = in.readInt();
		this.dayPayTimes = in.readInt();
		this.dayPayAmount = in.readFloat();
		this.totalOnlineDays = in.readInt();
		this.totalLoginTimes = in.readInt();
		this.totalOnlineTime = in.readInt();
		this.totalPayAmount = in.readFloat();
		this.totalPayTimes = in.readInt();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsString(out);
		super.writeSeparator(out);
		super.writeUTF8WithSep(out, isNewPlayer);
		super.writeUTF8WithSep(out, isPayPlayer);
		super.writeUTF8WithSep(out, isPayAtFirstDay);
		super.writeUTF8WithSep(out, isEverPayPlayer);
		super.writeUTF8WithSep(out, isNewAndCreateRole);
		super.writeUTF8WithSep(out, dayLoginTimes);
		super.writeUTF8WithSep(out, dayOnlineTime);
		super.writeUTF8WithSep(out, dayPayTimes);
		super.writeUTF8WithSep(out, dayPayAmount);
		super.writeUTF8WithSep(out, totalOnlineDays);
		super.writeUTF8WithSep(out, totalLoginTimes);
		super.writeUTF8WithSep(out, totalOnlineTime);
		super.writeUTF8WithSep(out, totalPayAmount);
		super.writeUTF8(out, totalPayTimes);
	}
}
