package com.dataeye.hadoop.domain.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;
import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.RawLog.Event;
import com.dataeye.hadoop.domain.common.RawLog.Header;

public class MKAccountId extends DEWritable {

	private String appId;
	private String platform;
	private String gameServer;
	private String accountId;
	
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

	public MKAccountId() {
	}

	public MKAccountId(Header header, Event event) {
		this.appId = header.getAppId();
		this.platform = header.getPlat();
		this.gameServer = event.getGameServer();
		this.accountId = event.getAccountId();
	}
	
	public MKAccountId(String appId, String platform, String gameServer, String accountId) {
		this.appId = appId;
		this.platform = platform;
		this.gameServer = gameServer;
		this.accountId = accountId;
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
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.appId = in.readUTF();
		this.platform = in.readUTF();
		this.gameServer = in.readUTF();
		this.accountId = in.readUTF();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeUTF8WithSep(out, appId);
		super.writeUTF8WithSep(out, platform);
		super.writeUTF8WithSep(out, gameServer);
		super.writeUTF8(out, accountId);
	}
}
