package com.dataeye.hadoop.domain.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.dataeye.hadoop.domain.common.MRConstants;
import com.dataeye.hadoop.domain.common.RoleBaseWritable;
import com.dataeye.hadoop.util.StringUtil;

public class RoleHistoryInfo extends RoleBaseWritable {

	// 创角时间
	private int createTime;
	// 激活时间
	private int enableTime;
	// 删除时间
	private int deleteTime;

	// 首登日期
	private int firstLoginDate;
	// 首次登录时间
	private int firstLoginTime;
	// 最后登录日期
	private int lastLoginDate;
	// 最后登录时间
	private int lastLoginTime;
	// 总在线天数
	private int totalOnlineDays;
	// 总登录次数
	private int totalLoginTimes;
	// 总在线时长
	private int totalOnlineTime;

	// 首付日期
	private int firstPayDate;
	// 首付时间
	private int firstPayTime;
	// 首次付费金额
	private float firstPayAmount;
	// 最后付费日期
	private int lastPayDate;
	// 最后一次付费日期
	private int lastPayTime;
	// 最后一次付费金额
	private float lastPayAmount;
	// 总付费金额
	private float totalPayAmount;
	// 总付费次数
	private int totalPayTimes;

	/*
	 * 记录玩家生命周期里每天的登录标记 标记以玩家登录天减去新增天的天数差作为值存放，如： 玩家 1 号新增，3 号登录，那么 3-1=2，则 olTrack 中有 2 存在
	 */
	private String olTrack = MRConstants.STR_EMPTY_SET_JSON;
	// 记录玩家生命周期里每天的付费标记，原理同 olTrack
	private String payTrack = MRConstants.STR_EMPTY_SET_JSON;

	// // 用于解析 olTrack，即 set 中放了登录天-新增天的天数差
	// private Set<Integer> olDaysSet;
	// // 作用同 olDaysSet
	// private Set<Integer> payDaysSet;

	public int getFirstLoginDate() {
		return firstLoginDate;
	}

	public void setFirstLoginDate(int firstLoginDate) {
		this.firstLoginDate = firstLoginDate;
	}

	public int getFirstLoginTime() {
		return firstLoginTime;
	}

	public void setFirstLoginTime(int firstLoginTime) {
		this.firstLoginTime = firstLoginTime;
	}

	public int getLastLoginDate() {
		return lastLoginDate;
	}

	public void setLastLoginDate(int lastLoginDate) {
		this.lastLoginDate = lastLoginDate;
	}

	public int getLastLoginTime() {
		return lastLoginTime;
	}

	public void setLastLoginTime(int lastLoginTime) {
		this.lastLoginTime = lastLoginTime;
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

	public int getFirstPayDate() {
		return firstPayDate;
	}

	public void setFirstPayDate(int firstPayDate) {
		this.firstPayDate = firstPayDate;
	}

	public int getFirstPayTime() {
		return firstPayTime;
	}

	public void setFirstPayTime(int firstPayTime) {
		this.firstPayTime = firstPayTime;
	}

	public int getLastPayDate() {
		return lastPayDate;
	}

	public void setLastPayDate(int lastPayDate) {
		this.lastPayDate = lastPayDate;
	}

	public int getLastPayTime() {
		return lastPayTime;
	}

	public void setLastPayTime(int lastPayTime) {
		this.lastPayTime = lastPayTime;
	}

	public float getLastPayAmount() {
		return lastPayAmount;
	}

	public void setLastPayAmount(float lastPayAmount) {
		this.lastPayAmount = lastPayAmount;
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

	public String getOlTrack() {
		return olTrack;
	}

	public void setOlTrack(String olTrack) {
		this.olTrack = olTrack;
	}

	public String getPayTrack() {
		return payTrack;
	}

	public void setPayTrack(String payTrack) {
		this.payTrack = payTrack;
	}

	public float getFirstPayAmount() {
		return firstPayAmount;
	}

	public void setFirstPayAmount(float firstPayAmount) {
		this.firstPayAmount = firstPayAmount;
	}

	// public Set<Integer> getOlDaysSet() {
	// return olDaysSet;
	// }
	//
	// public void setOlDaysSet(Set<Integer> olDaysSet) {
	// this.olDaysSet = olDaysSet;
	// }
	//
	// public Set<Integer> getPayDaysSet() {
	// return payDaysSet;
	// }
	//
	// public void setPayDaysSet(Set<Integer> payDaysSet) {
	// this.payDaysSet = payDaysSet;
	// }

	public int getCreateTime() {
		return createTime;
	}

	public void setCreateTime(int createTime) {
		this.createTime = createTime;
	}

	public int getEnableTime() {
		return enableTime;
	}

	public void setEnableTime(int enableTime) {
		this.enableTime = enableTime;
	}

	public int getDeleteTime() {
		return deleteTime;
	}

	public void setDeleteTime(int deleteTime) {
		this.deleteTime = deleteTime;
	}

	public RoleHistoryInfo() {

	}

	public RoleHistoryInfo(RoleBaseWritable roleBase) {
		super(roleBase);
	}

	public RoleHistoryInfo(RoleHistoryInfo histInfo) {
		super(histInfo);

		this.createTime = histInfo.getCreateTime();
		this.enableTime = histInfo.getEnableTime();
		this.deleteTime = histInfo.getDeleteTime();

		this.firstLoginDate = histInfo.getFirstLoginDate();
		this.firstLoginTime = histInfo.getFirstLoginTime();
		this.lastLoginDate = histInfo.getLastLoginDate();
		this.lastLoginTime = histInfo.getLastLoginTime();
		this.totalOnlineDays = histInfo.getTotalOnlineDays();
		this.totalLoginTimes = histInfo.getTotalLoginTimes();
		this.totalOnlineTime = histInfo.getTotalOnlineTime();

		this.firstPayDate = histInfo.getFirstPayDate();
		this.firstPayTime = histInfo.getFirstPayTime();
		this.firstPayAmount = histInfo.getFirstPayAmount();
		this.lastPayDate = histInfo.getLastPayDate();
		this.lastPayTime = histInfo.getLastPayTime();
		this.lastPayAmount = histInfo.getLastPayAmount();
		this.totalPayAmount = histInfo.getTotalPayAmount();
		this.totalPayTimes = histInfo.getTotalPayTimes();

		this.olTrack = histInfo.getOlTrack();
		this.payTrack = histInfo.getPayTrack();
	}

	public RoleHistoryInfo(String[] fields) {
		super(fields);
		this.createTime = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.enableTime = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.deleteTime = StringUtil.convertInt(fields[fieldsIndex++], 0);

		this.firstLoginDate = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.firstLoginTime = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.lastLoginDate = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.lastLoginTime = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.totalOnlineDays = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.totalLoginTimes = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.totalOnlineTime = StringUtil.convertInt(fields[fieldsIndex++], 0);

		this.firstPayDate = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.firstPayTime = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.firstPayAmount = StringUtil.convertFloat(fields[fieldsIndex++], 0);
		this.lastPayDate = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.lastPayTime = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.totalPayTimes = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.lastPayAmount = StringUtil.convertFloat(fields[fieldsIndex++], 0);
		this.totalPayAmount = StringUtil.convertFloat(fields[fieldsIndex++], 0);

		this.olTrack = fields[fieldsIndex++];
		this.payTrack = fields[fieldsIndex++];
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(createTime);
		out.writeInt(enableTime);
		out.writeInt(deleteTime);

		out.writeInt(firstLoginDate);
		out.writeInt(firstLoginTime);
		out.writeInt(lastLoginDate);
		out.writeInt(lastLoginTime);
		out.writeInt(totalOnlineDays);
		out.writeInt(totalLoginTimes);
		out.writeInt(totalOnlineTime);

		out.writeInt(firstPayDate);
		out.writeInt(firstPayTime);
		out.writeFloat(firstPayAmount);
		out.writeInt(lastPayDate);
		out.writeInt(lastPayTime);
		out.writeFloat(lastPayAmount);
		out.writeFloat(totalPayAmount);
		out.writeInt(totalPayTimes);

		out.writeUTF(olTrack);
		out.writeUTF(payTrack);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.createTime = in.readInt();
		this.enableTime = in.readInt();
		this.deleteTime = in.readInt();
		// history info
		this.firstLoginDate = in.readInt();
		this.firstLoginTime = in.readInt();
		this.lastLoginDate = in.readInt();
		this.lastLoginTime = in.readInt();
		this.totalOnlineDays = in.readInt();
		this.totalLoginTimes = in.readInt();
		this.totalOnlineTime = in.readInt();
		this.firstPayDate = in.readInt();
		this.firstPayTime = in.readInt();
		this.firstPayAmount = in.readFloat();
		this.lastPayDate = in.readInt();
		this.lastPayTime = in.readInt();
		this.lastPayAmount = in.readFloat();
		this.totalPayAmount = in.readFloat();
		this.totalPayTimes = in.readInt();
		this.olTrack = in.readUTF();
		this.payTrack = in.readUTF();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		// basic info
		super.writeAsString(out);
		super.writeSeparator(out);
		super.writeUTF8WithSep(out, createTime);
		super.writeUTF8WithSep(out, enableTime);
		super.writeUTF8WithSep(out, deleteTime);
		// history info
		super.writeUTF8WithSep(out, firstLoginDate);
		super.writeUTF8WithSep(out, firstLoginTime);
		super.writeUTF8WithSep(out, lastLoginDate);
		super.writeUTF8WithSep(out, lastLoginTime);
		super.writeUTF8WithSep(out, totalOnlineDays);
		super.writeUTF8WithSep(out, totalLoginTimes);
		super.writeUTF8WithSep(out, totalOnlineTime);
		super.writeUTF8WithSep(out, firstPayDate);
		super.writeUTF8WithSep(out, firstPayTime);
		super.writeUTF8WithSep(out, firstPayAmount);
		super.writeUTF8WithSep(out, lastPayDate);
		super.writeUTF8WithSep(out, lastPayTime);
		super.writeUTF8WithSep(out, lastPayAmount);
		super.writeUTF8WithSep(out, totalPayAmount);
		super.writeUTF8WithSep(out, totalPayTimes);
		super.writeUTF8WithSep(out, olTrack);
		super.writeUTF8(out, payTrack);
	}

	public void setHistoryInfo(RoleHistoryInfo histInfo) {
		this.createTime = histInfo.getCreateTime();
		this.enableTime = histInfo.getEnableTime();
		this.deleteTime = histInfo.getDeleteTime();

		this.firstLoginDate = histInfo.getFirstLoginDate();
		this.firstLoginTime = histInfo.getFirstLoginTime();
		this.lastLoginDate = histInfo.getLastLoginDate();
		this.lastLoginTime = histInfo.getLastLoginTime();
		this.totalOnlineDays = histInfo.getTotalOnlineDays();
		this.totalLoginTimes = histInfo.getTotalLoginTimes();
		this.totalOnlineTime = histInfo.getTotalOnlineTime();

		this.firstPayDate = histInfo.getFirstPayDate();
		this.firstPayTime = histInfo.getFirstPayTime();
		this.firstPayAmount = histInfo.getFirstPayAmount();
		this.lastPayDate = histInfo.getLastPayDate();
		this.lastPayTime = histInfo.getLastPayTime();
		this.lastPayAmount = histInfo.getLastPayAmount();
		this.totalPayAmount = histInfo.getTotalPayAmount();
		this.totalPayTimes = histInfo.getTotalPayTimes();

		this.olTrack = histInfo.getOlTrack();
		this.payTrack = histInfo.getPayTrack();
	}
}
