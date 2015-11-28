package com.dataeye.hadoop.domain.kv.role;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.MRConstants;
import com.dataeye.hadoop.domain.common.RoleBaseWritable;
import com.dataeye.hadoop.domain.hbase.RoleHistoryInfo;
import com.dataeye.hadoop.util.StringUtil;

public class RoleRollInfo extends RoleHistoryInfo {
	// 是否新创建
	private int isNewCreate;
	// 滚存日期
	private int untilDate;
	// 日登录次数
	private int dayLoginTimes;
	// 日在线时长
	private int dayOnlineTime;
	// 日付费次数
	private int dayPayTimes;
	// 日付费金额
	private float dayPayAmount;
	// 日在线明细
	private String dayOlDetail = MRConstants.STR_EMPTY_MAP_JSON;
	// 日付费明细
	private String dayPayDetail = MRConstants.STR_EMPTY_MAP_JSON;

	public int getUntilDate() {
		return untilDate;
	}

	public void setUntilDate(int untilDate) {
		this.untilDate = untilDate;
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

	public String getDayOlDetail() {
		return dayOlDetail;
	}

	public void setDayOlDetail(String dayOlDetail) {
		this.dayOlDetail = dayOlDetail;
	}

	public String getDayPayDetail() {
		return dayPayDetail;
	}

	public void setDayPayDetail(String dayPayDetail) {
		this.dayPayDetail = dayPayDetail;
	}

	public int getIsNewCreate() {
		return isNewCreate;
	}

	public void setIsNewCreate(int isNewCreate) {
		this.isNewCreate = isNewCreate;
	}

	public RoleRollInfo() {
	}

	public RoleRollInfo(RoleBaseWritable roleBase) {
		super(roleBase);
	}

	public RoleRollInfo(RoleRollInfo roleRoll) {
		super(roleRoll);
		this.isNewCreate = roleRoll.getIsNewCreate();
		this.untilDate = roleRoll.getUntilDate();
		this.dayLoginTimes = roleRoll.getDayLoginTimes();
		this.dayOnlineTime = roleRoll.getDayOnlineTime();
		this.dayPayTimes = roleRoll.getDayPayTimes();
		this.dayPayAmount = roleRoll.getDayPayAmount();
		this.dayOlDetail = roleRoll.getDayOlDetail();
		this.dayPayDetail = roleRoll.getDayPayDetail();
	}
	
	public RoleRollInfo(String fields) {
		this(fields.split(MRConstants.FIELDS_SEPARATOR));
	}

	public RoleRollInfo(String[] fields) {
		super(fields);
		this.isNewCreate = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.untilDate = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.dayLoginTimes = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.dayOnlineTime = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.dayPayTimes = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.dayPayAmount = StringUtil.convertFloat(fields[fieldsIndex++], 0);
		this.dayOlDetail = fields[fieldsIndex++];
		this.dayPayDetail = fields[fieldsIndex++];
	}
	
	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// history info
		super.write(out);
		// day info
		out.writeInt(isNewCreate);
		out.writeInt(untilDate);
		out.writeInt(dayLoginTimes);
		out.writeInt(dayOnlineTime);
		out.writeInt(dayPayTimes);
		out.writeFloat(dayPayAmount);
		out.writeUTF(dayOlDetail);
		out.writeUTF(dayPayDetail);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// basic info
		super.readFields(in);
		// day info
		this.isNewCreate = in.readInt();
		this.untilDate = in.readInt();
		this.dayLoginTimes = in.readInt();
		this.dayOnlineTime = in.readInt();
		this.dayPayTimes = in.readInt();
		this.dayPayAmount = in.readFloat();
		this.dayOlDetail = in.readUTF();
		this.dayPayDetail = in.readUTF();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		// basic info
		super.writeAsString(out);
		// day info
		super.writeSeparator(out);
		super.writeUTF8WithSep(out, isNewCreate);
		super.writeUTF8WithSep(out, untilDate);
		super.writeUTF8WithSep(out, dayLoginTimes);
		super.writeUTF8WithSep(out, dayOnlineTime);
		super.writeUTF8WithSep(out, dayPayTimes);
		super.writeUTF8WithSep(out, dayPayAmount);
		super.writeUTF8WithSep(out, dayOlDetail);
		super.writeUTF8(out, dayPayDetail);
	}

	public void setHistoryInfo(RoleHistoryInfo histInfo, int dataDate) {
		if (null == histInfo) {
			return;
		}
		super.setHistoryInfo(histInfo);
		this.untilDate = dataDate;
	}
}
