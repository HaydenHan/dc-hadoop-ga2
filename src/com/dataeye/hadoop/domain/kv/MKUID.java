package com.dataeye.hadoop.domain.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;
import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.RawLog;
import com.dataeye.hadoop.domain.common.RawLog.Header;

public class MKUID extends DEWritable {

	private String appId;
	private String platform;
	private String uid;
	
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

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public MKUID() {
	}

	public MKUID(RawLog log) {
		this.appId = log.getHeader().getAppId();
		this.platform = log.getHeader().getPlat();
		this.uid = log.getHeader().getUid();
	}
	
	public MKUID(Header header) {
		this.appId = header.getAppId();
		this.platform = header.getPlat();
		this.uid = header.getUid();
	}
	
	public MKUID(String appId, String platform, String uid) {
		this.appId = appId;
		this.platform = platform;
		this.uid = uid;
	}
	
	public MKUID(String[] fields) {
		int i = 0;
		this.appId = fields[i++];
		this.platform = fields[i++];
		this.uid = fields[i++];
	}

	
	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(appId);
		out.writeUTF(platform);
		out.writeUTF(uid);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.appId = in.readUTF();
		this.platform = in.readUTF();
		this.uid = in.readUTF();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeUTF8WithSep(out, appId);
		super.writeUTF8WithSep(out, platform);
		super.writeUTF8(out, uid); // the final one don't need a separator
	}

	@Override
	public String toString() {
		return "UIDKey [appId=" + appId + ", platform=" + platform + ", uid="
				+ uid + "]";
	}
}
