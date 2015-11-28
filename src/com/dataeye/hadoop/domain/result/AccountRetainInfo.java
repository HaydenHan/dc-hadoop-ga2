package com.dataeye.hadoop.domain.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.AccountBaseWritable;
import com.dataeye.hadoop.domain.hbase.AccountHistoryInfo;

public class AccountRetainInfo extends AccountBaseWritable {

	private int isNewPlayer;
	private int isPayPlayer;
	private int isEverPayPlayer;
	private int actionTimes;
	private int targetDate;
	private int nDayRetain;

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

	public int getIsEverPayPlayer() {
		return isEverPayPlayer;
	}

	public void setIsEverPayPlayer(int isEverPayPlayer) {
		this.isEverPayPlayer = isEverPayPlayer;
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

	public AccountRetainInfo() {
	}

	public AccountRetainInfo(AccountHistoryInfo histInfo) {
		super(histInfo);
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
		out.writeInt(isEverPayPlayer);
		out.writeInt(actionTimes);
		out.writeInt(targetDate);
		out.writeInt(nDayRetain);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.isNewPlayer = in.readInt();
		this.isPayPlayer = in.readInt();
		this.isEverPayPlayer = in.readInt();
		this.actionTimes = in.readInt();
		this.targetDate = in.readInt();
		this.nDayRetain = in.readInt();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsString(out);
		super.writeSeparator(out);
		super.writeUTF8WithSep(out, isNewPlayer);
		super.writeUTF8WithSep(out, isPayPlayer);
		super.writeUTF8WithSep(out, isEverPayPlayer);
		super.writeUTF8WithSep(out, actionTimes);
		super.writeUTF8WithSep(out, targetDate);
		super.writeUTF8(out, nDayRetain);
	}

}
