package com.dataeye.hadoop.domain.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.RawLog.Event;
import com.dataeye.hadoop.domain.common.RawLog.Header;

/**
 * <pre>
 * 角色信息基类
 * @author Hayden<br>
 * @date 2015年11月5日 下午6:00:36
 * <br>
 */
public class RoleBaseWritable extends AccountBaseWritable {
	private String roleId = MRConstants.STR_PLACE_HOLDER;
	// private String roleName = MRConstants.STR_PLACE_HOLDER;
	// 种族
	private String roleRace = MRConstants.STR_PLACE_HOLDER;
	// 职业
	private String roleClass = MRConstants.STR_PLACE_HOLDER;
	private String roleLevel = MRConstants.STR_PLACE_HOLDER;

	public String getRoleId() {
		return roleId;
	}

	public void setRoleId(String roleId) {
		this.roleId = roleId;
	}

	// public String getRoleName() {
	// return roleName;
	// }
	//
	// public void setRoleName(String roleName) {
	// this.roleName = roleName;
	// }

	public String getRoleRace() {
		return roleRace;
	}

	public void setRoleRace(String roleRace) {
		this.roleRace = roleRace;
	}

	public String getRoleClass() {
		return roleClass;
	}

	public void setRoleClass(String roleClass) {
		this.roleClass = roleClass;
	}

	public String getRoleLevel() {
		return roleLevel;
	}

	public void setRoleLevel(String roleLevel) {
		this.roleLevel = roleLevel;
	}


	public RoleBaseWritable() {

	};

	public RoleBaseWritable(RoleBaseWritable roleBase) {
		super(roleBase);
		this.roleId = roleBase.roleId;
		// this.roleName = roleBase.roleName;
		this.roleRace = roleBase.roleRace;
		this.roleClass = roleBase.roleClass;
		this.roleLevel = roleBase.roleLevel;

	};

	public RoleBaseWritable(String[] fields) {
		super(fields);
		this.roleId = fields[fieldsIndex++];
		// this.roleName = fields[fieldsIndex++];
		this.roleRace = fields[fieldsIndex++];
		this.roleClass = fields[fieldsIndex++];
		this.roleLevel = fields[fieldsIndex++];
	};

	public RoleBaseWritable(Header header, Event event) {
		super(header, event);
		this.roleId = event.getRoleId();
		// this.roleName = event.getRoleName();
		this.roleRace = event.getRoleRace();
		this.roleClass = event.getRoleClass();
		this.roleLevel = event.getRoleLevel();
	}

	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeUTF(this.roleId);
		// out.writeUTF(this.roleName);
		out.writeUTF(this.roleRace);
		out.writeUTF(this.roleClass);
		out.writeUTF(this.roleLevel);
	}

	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.roleId = in.readUTF();
		// this.roleName = in.readUTF();
		this.roleRace = in.readUTF();
		this.roleClass = in.readUTF();
		this.roleLevel = in.readUTF();
	}

	public void writeAsString(DataOutput out) throws IOException {
		super.writeAsString(out);
		super.writeSeparator(out);
		super.writeUTF8WithSep(out, this.roleId);
		// super.writeUTF8WithSep(out, this.roleName);
		super.writeUTF8WithSep(out, this.roleRace);
		super.writeUTF8WithSep(out, this.roleClass);
		super.writeUTF8(out, this.roleLevel);
	}
}
