package com.dataeye.hadoop.domain.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.AccountBaseWritable;
import com.dataeye.hadoop.domain.common.RawLog.Event;
import com.dataeye.hadoop.domain.common.RawLog.Header;
import com.dataeye.hadoop.util.StringUtil;

public class MVAccountCreate extends AccountBaseWritable {

	private int createTime;
	
	public int getCreateTime() {
		return createTime;
	}

	public void setCreateTime(int createTime) {
		this.createTime = createTime;
	}

	public MVAccountCreate(){}

	public MVAccountCreate(MVAccountCreate accCreate){
		super(accCreate);
		this.createTime = accCreate.getCreateTime();
	}
	
	public MVAccountCreate(String[] fields){
		super(fields);
		this.createTime = StringUtil.convertInt(fields[fieldsIndex++], 0);
	}
	
	public MVAccountCreate(Header header, Event event) {
		super(header, event);
		this.createTime = event.getStartTime();
	}
	
	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(createTime);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.createTime = in.readInt();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsString(out);
		super.writeSeparator(out);
		super.writeUTF8(out, createTime);
	}
}
