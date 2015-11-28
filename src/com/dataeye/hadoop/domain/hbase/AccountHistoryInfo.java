package com.dataeye.hadoop.domain.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.AccountBaseWritable;
import com.dataeye.hadoop.domain.common.MRConstants;
import com.dataeye.hadoop.util.StringUtil;

public class AccountHistoryInfo extends AccountBaseWritable {

	// 注册时间
	private int createTime;
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
	 * 记录玩家生命周期里每天的登录标记 标记以玩家登录天减去新增天的天数差作为值存放， 如： 玩家 1 号新增，3 号登录，那么 3-1=2，则 olTrack 中有 2 存在
	 */
	private String olTrack = MRConstants.STR_EMPTY_SET_JSON;
	// 记录玩家生命周期里每天的付费标记，原理同 olTrack
	private String payTrack = MRConstants.STR_EMPTY_SET_JSON;
	// History Info
	private String historyUid = MRConstants.STR_EMPTY_SET_JSON;

	public int getCreateTime() {
		return createTime;
	}

	public void setCreateTime(int createTime) {
		this.createTime = createTime;
	}

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

	public String getHistoryUid() {
		return historyUid;
	}

	public void setHistoryUid(String historyUid) {
		this.historyUid = historyUid;
	}

	public AccountHistoryInfo() {
	};

	public AccountHistoryInfo(AccountBaseWritable accountBase) {
		super(accountBase);
	};

	public AccountHistoryInfo(String[] fields) {
		super(fields);
		this.createTime = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.firstLoginDate = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.firstLoginTime = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.lastLoginDate = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.lastLoginTime = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.totalOnlineDays = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.totalLoginTimes = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.totalOnlineTime = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.firstPayDate = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.firstPayTime = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.lastPayDate = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.lastPayTime = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.lastPayAmount = StringUtil.convertFloat(fields[fieldsIndex++], 0);
		this.totalPayAmount = StringUtil.convertFloat(fields[fieldsIndex++], 0);
		this.totalPayTimes = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.olTrack = fields[fieldsIndex++];
		this.payTrack = fields[fieldsIndex++];
		this.historyUid = fields[fieldsIndex++];
	};

	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(createTime);
		out.writeInt(firstLoginDate);
		out.writeInt(firstLoginTime);
		out.writeInt(lastLoginDate);
		out.writeInt(lastLoginTime);
		out.writeInt(totalOnlineDays);
		out.writeInt(totalLoginTimes);
		out.writeInt(totalOnlineTime);
		out.writeInt(firstPayDate);
		out.writeInt(firstPayTime);
		out.writeInt(lastPayDate);
		out.writeInt(lastPayTime);
		out.writeFloat(lastPayAmount);
		out.writeFloat(totalPayAmount);
		out.writeInt(totalPayTimes);
		out.writeUTF(olTrack);
		out.writeUTF(payTrack);
		out.writeUTF(historyUid);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.createTime = in.readInt();
		this.firstLoginDate = in.readInt();
		this.firstLoginTime = in.readInt();
		this.lastLoginDate = in.readInt();
		this.lastLoginTime = in.readInt();
		this.totalOnlineDays = in.readInt();
		this.totalLoginTimes = in.readInt();
		this.totalOnlineTime = in.readInt();
		this.firstPayDate = in.readInt();
		this.firstPayTime = in.readInt();
		this.lastPayDate = in.readInt();
		this.lastPayTime = in.readInt();
		this.lastPayAmount = in.readFloat();
		this.totalPayAmount = in.readFloat();
		this.totalPayTimes = in.readInt();
		this.olTrack = in.readUTF();
		this.payTrack = in.readUTF();
		this.historyUid = in.readUTF();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsString(out);
		super.writeSeparator(out);
		super.writeUTF8WithSep(out, createTime);
		super.writeUTF8WithSep(out, firstLoginDate);
		super.writeUTF8WithSep(out, firstLoginTime);
		super.writeUTF8WithSep(out, lastLoginDate);
		super.writeUTF8WithSep(out, lastLoginTime);
		super.writeUTF8WithSep(out, totalOnlineDays);
		super.writeUTF8WithSep(out, totalLoginTimes);
		super.writeUTF8WithSep(out, totalOnlineTime);
		super.writeUTF8WithSep(out, firstPayDate);
		super.writeUTF8WithSep(out, firstPayTime);
		super.writeUTF8WithSep(out, lastPayDate);
		super.writeUTF8WithSep(out, lastPayTime);
		super.writeUTF8WithSep(out, lastPayAmount);
		super.writeUTF8WithSep(out, totalPayAmount);
		super.writeUTF8WithSep(out, totalPayTimes);
		super.writeUTF8WithSep(out, olTrack);
		super.writeUTF8WithSep(out, payTrack);
		super.writeUTF8(out, historyUid);
	}
}
