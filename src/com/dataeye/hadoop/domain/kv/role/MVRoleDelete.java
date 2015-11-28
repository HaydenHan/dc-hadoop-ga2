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

public class MVRoleDelete extends RoleBaseWritable {

	private int deleteTime;

	public int getDeleteTime() {
		return deleteTime;
	}

	public void setDeleteTime(int deleteTime) {
		this.deleteTime = deleteTime;
	}


	public MVRoleDelete() {
	}

	public MVRoleDelete(MVRoleDelete accCreate) {
		super(accCreate);
		this.deleteTime = accCreate.getDeleteTime();
	}

	public MVRoleDelete(String[] fields) {
		super(fields);
		this.deleteTime = StringUtil.convertInt(fields[fieldsIndex++], 0);
	}

	public MVRoleDelete(Header header, Event event) {
		super(header, event);
		this.deleteTime = event.getStartTime();
	}

	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(deleteTime);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.deleteTime = in.readInt();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsString(out);
		super.writeSeparator(out);
		super.writeAsUTF8Bytes(out, deleteTime);
	}

}
