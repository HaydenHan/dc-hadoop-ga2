package com.dataeye.hadoop.domain.result.role;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.RoleBaseWritable;
import com.dataeye.hadoop.domain.hbase.RoleHistoryInfo;
import com.dataeye.hadoop.domain.kv.role.RoleRollInfo;

public class RoleRetainInfo extends RoleBaseWritable {
	private int isNewRole;  //用于统计当天新增角色数
	private int isPayRole; //针对留存当天是否付费
	private int isEverPayRole; //是否曾经付过费，不单留存当天
	private int actionTimes; //特定动作发生次数（如登录次数或在线天数）	（待定）
	private int targetDate; //统计日
	private int nDayRetain; // N 天留存

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

	public int getIsEverPayRole() {
		return isEverPayRole;
	}

	public void setIsEverPayRole(int isEverPayRole) {
		this.isEverPayRole = isEverPayRole;
	}

	public int getActionTimes() {
		return actionTimes;
	}

	public void setActionTimes(int actionTimes) {
		this.actionTimes = actionTimes;
	}

	public int getTargetDate() {
		return targetDate;
	}

	public void setTargetDate(int targetDate) {
		this.targetDate = targetDate;
	}

	public int getnDayRetain() {
		return nDayRetain;
	}

	public void setnDayRetain(int nDayRetain) {
		this.nDayRetain = nDayRetain;
	}

	public RoleRetainInfo() {

	}

	public RoleRetainInfo(RoleRollInfo rollInfo) {
		super(rollInfo);
	}

	public RoleRetainInfo(RoleHistoryInfo roleHistry) {
		super(roleHistry);
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
		out.writeInt(isEverPayRole);
		out.writeInt(actionTimes);
		out.writeInt(targetDate);
		out.writeInt(nDayRetain);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.isNewRole = in.readInt();
		this.isPayRole = in.readInt();
		this.isEverPayRole = in.readInt();
		this.actionTimes = in.readInt();
		this.targetDate = in.readInt();
		this.nDayRetain = in.readInt();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsString(out);
		super.writeSeparator(out);
		super.writeUTF8WithSep(out, this.isNewRole);
		super.writeUTF8WithSep(out, this.isPayRole);
		super.writeUTF8WithSep(out, this.isEverPayRole);
		super.writeUTF8WithSep(out, this.actionTimes);
		super.writeUTF8WithSep(out, this.targetDate);
		super.writeUTF8(out, this.nDayRetain);
	}
}
