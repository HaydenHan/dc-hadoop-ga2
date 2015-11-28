package com.dataeye.hadoop.domain.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.DEHeaderWritable;
import com.dataeye.hadoop.domain.common.RawLog.Event;
import com.dataeye.hadoop.domain.common.RawLog.Header;
import com.dataeye.hadoop.util.StringUtil;

public class MVUIDActive extends DEHeaderWritable {

	private int activeTime;
	// 每次激活时通过缓存
	private int isRealActive;

	public int getActiveTime() {
		return activeTime;
	}

	public void setActiveTime(int activeTime) {
		this.activeTime = activeTime;
	}

	public int getIsRealActive() {
		return isRealActive;
	}

	public void setIsRealActive(int isRealActive) {
		this.isRealActive = isRealActive;
	}

	public MVUIDActive() {
	}

	public MVUIDActive(MVUIDActive active) {
		super(active);
		this.activeTime = active.activeTime;
		this.isRealActive = active.isRealActive;
	}

	public MVUIDActive(String[] fields) {
		super(fields);
		this.activeTime = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.isRealActive = StringUtil.convertInt(fields[fieldsIndex++], 0);
	}

	public MVUIDActive(Header header, Event event) {
		super(header, event);
		this.activeTime = event.getStartTime();
	}

	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(activeTime);
		out.writeInt(isRealActive);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.activeTime = in.readInt();
		this.isRealActive = in.readInt();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsString(out);
		super.writeSeparator(out);
		super.writeUTF8WithSep(out, activeTime);
		super.writeUTF8(out, isRealActive);
	}

}
