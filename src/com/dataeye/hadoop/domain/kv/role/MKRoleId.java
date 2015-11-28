package com.dataeye.hadoop.domain.kv.role;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.RawLog;
import com.dataeye.hadoop.domain.common.RawLog.Event;
import com.dataeye.hadoop.domain.common.RawLog.Header;

public class MKRoleId extends DEWritable {

	private String appId;
	private String platform;
	private String gameServer;
	private String accountId;
	private String roleId;

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public String getGameServer() {
		return gameServer;
	}

	public void setGameServer(String gameServer) {
		this.gameServer = gameServer;
	}

	public String getAccountId() {
		return accountId;
	}

	public void setAccountId(String accountId) {
		this.accountId = accountId;
	}

	public String getRoleId() {
		return roleId;
	}

	public void setRoleId(String roleId) {
		this.roleId = roleId;
	}

	public MKRoleId() {
	}

	public MKRoleId(RawLog log, Event event) {
		this(log.getHeader(), event);
	}

	public MKRoleId(Header header, Event event) {
		this.appId = header.getAppId();
		this.platform = header.getPlat();
		this.accountId = event.getAccountId();
		this.gameServer = event.getGameServer();
		this.roleId = event.getRoleId();
	}

	public MKRoleId(String appId, String platform, String gameServer, String accountId, String roleId) {
		this.appId = appId;
		this.platform = platform;
		this.gameServer = gameServer;
		this.accountId = accountId;
		this.roleId = roleId;
	}

	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(appId);
		out.writeUTF(platform);
		out.writeUTF(gameServer);
		out.writeUTF(accountId);
		out.writeUTF(roleId);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.appId = in.readUTF();
		this.platform = in.readUTF();
		this.gameServer = in.readUTF();
		this.accountId = in.readUTF();
		this.roleId = in.readUTF();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeUTF8WithSep(out, appId);
		super.writeUTF8WithSep(out, platform);
		super.writeUTF8WithSep(out, gameServer);
		super.writeUTF8WithSep(out, accountId);
		super.writeAsUTF8Bytes(out, roleId);
	}
}
