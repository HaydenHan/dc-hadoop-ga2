package com.dataeye.hadoop.domain.result.role;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.RoleBaseWritable;
import com.dataeye.hadoop.domain.kv.role.RoleRollInfo;

public class RoleDayInfo extends RoleBaseWritable {

	private int isNewRole;
	private int isPayRole;
	private int isPayAtFirstDay;
	private int isEverPayRole;
	
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

	public int getIsNewRole() {
		return isNewRole;
	}

	public void setIsNewRole(int isNewRole) {
		this.isNewRole = isNewRole;
	}

	public int getIsPayRole() {
		return isPayRole;
	}

	public void setIsPayRole(int isPayRole) {
		this.isPayRole = isPayRole;
	}

	public int getIsPayAtFirstDay() {
		return isPayAtFirstDay;
	}

	public void setIsPayAtFirstDay(int isPayAtFirstDay) {
		this.isPayAtFirstDay = isPayAtFirstDay;
	}

	public int getIsEverPayRole() {
		return isEverPayRole;
	}

	public void setIsEverPayRole(int isEverPayRole) {
		this.isEverPayRole = isEverPayRole;
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

	public RoleDayInfo() {

	}

	public RoleDayInfo(RoleRollInfo rollInfo) {
		super(rollInfo);
	}

	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(isNewRole);
		out.writeInt(isPayRole);
		out.writeInt(isPayAtFirstDay);
		out.writeInt(isEverPayRole);
		
		out.writeInt(dayLoginTimes);
		out.writeInt(dayOnlineTime);
		out.writeInt(dayPayTimes);
		out.writeFloat(dayPayAmount);
		
		// 总在线天数
		out.writeInt(totalOnlineDays);
		// 总登录次数
		out.writeInt(totalLoginTimes);
		// 总在线时长
		out.writeInt(totalOnlineTime);
		// 总付费金额
		out.writeFloat(totalPayAmount);
		// 总付费次数
		out.writeInt(totalPayTimes);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.isNewRole = in.readInt();
		this.isPayRole = in.readInt();
		this.isPayAtFirstDay = in.readInt();
		this.isEverPayRole = in.readInt();

		this.dayLoginTimes = in.readInt();
		this.dayOnlineTime = in.readInt();
		this.dayPayTimes = in.readInt();
		this.dayPayAmount = in.readFloat();

		// 总在线天数
		this.totalOnlineDays = in.readInt();
		// 总登录次数
		this.totalLoginTimes = in.readInt();
		// 总在线时长
		this.totalOnlineTime = in.readInt();
		// 总付费金额
		this.totalPayAmount = in.readFloat();
		// 总付费次数
		this.totalPayTimes = in.readInt();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsString(out);
		super.writeSeparator(out);
		super.writeUTF8WithSep(out, this.isNewRole);
		super.writeUTF8WithSep(out, this.isPayRole);
		super.writeUTF8WithSep(out, this.isPayAtFirstDay);
		super.writeUTF8WithSep(out, this.isEverPayRole);

		super.writeUTF8WithSep(out, this.dayLoginTimes);
		super.writeUTF8WithSep(out, this.dayOnlineTime);
		super.writeUTF8WithSep(out, this.dayPayTimes);
		super.writeUTF8WithSep(out, this.dayPayAmount);

		super.writeUTF8WithSep(out, this.totalOnlineDays);
		super.writeUTF8WithSep(out, this.totalLoginTimes);
		super.writeUTF8WithSep(out, this.totalOnlineTime);
		super.writeUTF8WithSep(out, this.totalPayAmount);
		super.writeUTF8(out, this.totalPayTimes);
	}
}
