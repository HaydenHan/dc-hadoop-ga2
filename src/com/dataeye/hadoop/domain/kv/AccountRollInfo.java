package com.dataeye.hadoop.domain.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.AccountBaseWritable;
import com.dataeye.hadoop.domain.common.MRConstants;
import com.dataeye.hadoop.domain.hbase.AccountHistoryInfo;
import com.dataeye.hadoop.util.StringUtil;

public class AccountRollInfo extends AccountHistoryInfo {

	private int dayCreateTime;
	private int dayLoginTimes;
	private int dayOnlineTime;
	private int dayPayTimes;
	private float dayPayAmount;
	private String dayOlDetail = MRConstants.STR_EMPTY_MAP_JSON;
	private String dayPayDetail = MRConstants.STR_EMPTY_MAP_JSON;
	private String dayRoleIds = MRConstants.STR_PLACE_HOLDER;

	public int getDayCreateTime() {
		return dayCreateTime;
	}

	public void setDayCreateTime(int dayCreateTime) {
		this.dayCreateTime = dayCreateTime;
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

	public String getDayRoleIds() {
		return dayRoleIds;
	}

	public void setDayRoleIds(String dayRoleIds) {
		this.dayRoleIds = dayRoleIds;
	}

	public AccountRollInfo() {
	}

	public AccountRollInfo(AccountBaseWritable accountBase) {
		super(accountBase);
	}

	public AccountRollInfo(String fields) {
		this(fields.split(MRConstants.FIELDS_SEPARATOR));
	}

	public AccountRollInfo(String[] fields) {
		super(fields);
		this.dayCreateTime = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.dayLoginTimes = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.dayOnlineTime = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.dayPayTimes = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.dayPayAmount = StringUtil.convertFloat(fields[fieldsIndex++], 0);
		this.dayOlDetail = fields[fieldsIndex++];
		this.dayPayDetail = fields[fieldsIndex++];
		this.dayRoleIds = fields[fieldsIndex++];
	}

	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(dayCreateTime);
		out.writeInt(dayLoginTimes);
		out.writeInt(dayOnlineTime);
		out.writeInt(dayPayTimes);
		out.writeFloat(dayPayAmount);
		out.writeUTF(dayOlDetail);
		out.writeUTF(dayPayDetail);
		out.writeUTF(dayRoleIds);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.dayCreateTime = in.readInt();
		this.dayLoginTimes = in.readInt();
		this.dayOnlineTime = in.readInt();
		this.dayPayTimes = in.readInt();
		this.dayPayAmount = in.readFloat();
		this.dayOlDetail = in.readUTF();
		this.dayPayDetail = in.readUTF();
		this.dayRoleIds = in.readUTF();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsString(out);
		super.writeSeparator(out);
		super.writeUTF8WithSep(out, dayCreateTime);
		super.writeUTF8WithSep(out, dayLoginTimes);
		super.writeUTF8WithSep(out, dayOnlineTime);
		super.writeUTF8WithSep(out, dayPayTimes);
		super.writeUTF8WithSep(out, dayPayAmount);
		super.writeUTF8WithSep(out, dayOlDetail);
		super.writeUTF8WithSep(out, dayPayDetail);
		super.writeUTF8(out, dayRoleIds);
	}

}
