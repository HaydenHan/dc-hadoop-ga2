package com.dataeye.hadoop.domain.kv.role;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.RawLog.Event;
import com.dataeye.hadoop.domain.common.RawLog.Header;
import com.dataeye.hadoop.domain.common.RoleBaseWritable;
import com.dataeye.hadoop.util.StringUtil;

public class MVRoleEnable extends RoleBaseWritable {

	private int enableTime;
	

	public int getEnableTime() {
		return enableTime;
	}

	public void setEnableTime(int enableTime) {
		this.enableTime = enableTime;
	}


	public MVRoleEnable(){}

	public MVRoleEnable(MVRoleEnable accCreate){
		super(accCreate);
		this.enableTime = accCreate.getEnableTime();
	}
	
	public MVRoleEnable(String[] fields){
		super(fields);
		this.enableTime = StringUtil.convertInt(fields[fieldsIndex++], 0);
	}
	
	public MVRoleEnable(Header header, Event event) {
		super(header, event);
		this.enableTime = event.getStartTime();
	}
	
	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(enableTime);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.enableTime = in.readInt();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsString(out);
		super.writeSeparator(out);
		super.writeAsUTF8Bytes(out, enableTime);
	}

}
