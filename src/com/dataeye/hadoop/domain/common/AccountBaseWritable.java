package com.dataeye.hadoop.domain.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.RawLog.Event;
import com.dataeye.hadoop.domain.common.RawLog.Header;

public class AccountBaseWritable extends DEHeaderWritable {

	private String accountId = MRConstants.STR_PLACE_HOLDER; // 账户 id
	private String accountType = MRConstants.STR_PLACE_HOLDER; // 账户类型
	private String gender = MRConstants.STR_PLACE_HOLDER; // 性别
	private String age = MRConstants.STR_PLACE_HOLDER; // 年龄
	private String gameServer = MRConstants.STR_PLACE_HOLDER; // 区服
	private String fromServer = MRConstants.STR_PLACE_HOLDER; // 区服

	public String getGameServer() {
		return gameServer;
	}

	public void setGameServer(String gameServer) {
		this.gameServer = gameServer;
	}

	public String getFromServer() {
		return fromServer;
	}

	public void setFromServer(String fromServer) {
		this.fromServer = fromServer;
	}

	public String getAccountId() {
		return accountId;
	}

	public void setAccountId(String accountId) {
		this.accountId = accountId;
	}

	public String getAccountType() {
		return accountType;
	}

	public void setAccountType(String accountType) {
		this.accountType = accountType;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public String getAge() {
		return age;
	}

	public void setAge(String age) {
		this.age = age;
	}

	public AccountBaseWritable() {
	};

	public AccountBaseWritable(AccountBaseWritable accountBase) {
		super(accountBase);
		this.gameServer = accountBase.gameServer;
		this.fromServer = accountBase.fromServer;
		this.accountId = accountBase.accountId;
		this.accountType = accountBase.accountType;
		this.gender = accountBase.gender;
		this.age = accountBase.age;
	};

	public AccountBaseWritable(String[] fields) {
		super(fields);
		this.accountId = fields[fieldsIndex++];
		this.accountType = fields[fieldsIndex++];
		this.gender = fields[fieldsIndex++];
		this.age = fields[fieldsIndex++];
		this.gameServer = fields[fieldsIndex++];
		this.fromServer = fields[fieldsIndex++];
	};

	public AccountBaseWritable(Header header, Event event) {
		super(header, event);
		this.gameServer = event.getGameServer();
		this.fromServer = event.getGameServer();
		this.accountId = event.getAccountId();
		this.accountType = event.getAccountType();
		this.gender = event.getAccountGender();
		this.age = event.getAccountAge();
	}

	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeUTF(accountId);
		out.writeUTF(accountType);
		out.writeUTF(gender);
		out.writeUTF(age);
		out.writeUTF(gameServer);
		out.writeUTF(fromServer);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.accountId = in.readUTF();
		this.accountType = in.readUTF();
		this.gender = in.readUTF();
		this.age = in.readUTF();
		this.gameServer = in.readUTF();
		this.fromServer = in.readUTF();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsString(out);
		super.writeSeparator(out);
		super.writeUTF8WithSep(out, accountId);
		super.writeUTF8WithSep(out, accountType);
		super.writeUTF8WithSep(out, gender);
		super.writeUTF8WithSep(out, age);
		super.writeUTF8WithSep(out, gameServer);
		super.writeUTF8(out, fromServer);
	}
}
