package com.dataeye.hadoop.domain.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.AccountBaseWritable;
import com.dataeye.hadoop.domain.common.RawLog.Event;
import com.dataeye.hadoop.domain.common.RawLog.Header;

public class MVAccountOnline extends AccountBaseWritable {

	private int loginTime;
	private int onlineTime;
	
	public int getLoginTime() {
		return loginTime;
	}

	public void setLoginTime(int loginTime) {
		this.loginTime = loginTime;
	}

	public int getOnlineTime() {
		return onlineTime;
	}

	public void setOnlineTime(int onlineTime) {
		this.onlineTime = onlineTime;
	}

	public MVAccountOnline() {
	}

	public MVAccountOnline(MVAccountOnline accOnline) {
		super(accOnline);
		this.loginTime = accOnline.getLoginTime();
		this.onlineTime = accOnline.getOnlineTime();
	}
	
	public MVAccountOnline(Header header, Event event) {
		super(header, event);
		this.loginTime = event.getLoginTime();
		this.onlineTime = event.getOnlineTime();
	}
	
	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(loginTime);
		out.writeInt(onlineTime);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.loginTime = in.readInt();
		this.onlineTime = in.readInt();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsString(out);
		super.writeSeparator(out);
		super.writeUTF8WithSep(out, loginTime);
		super.writeUTF8(out, onlineTime);
	}
}
