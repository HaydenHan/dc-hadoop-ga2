package com.dataeye.hadoop.domain.hbase;

import org.apache.hadoop.hbase.util.Bytes;

public class RoleHistHBaseInfo extends HBaseBasicInfo {

	public static final String SEPARATOR = "|";
	public static final byte[] role_history_info = Bytes.toBytes("role_history_info");
	public static final byte[] cf_static = Bytes.toBytes("stac");
	public static final byte[] cf_dynamic = Bytes.toBytes("dymc");

	public static final byte[] d_q_gameServer = Bytes.toBytes("gameServer"); // 区服
	public static final byte[] d_q_fromServer = Bytes.toBytes("fromServer"); // 区服
	public static final byte[] d_q_accountId = Bytes.toBytes("accountId"); // 账户 id
	public static final byte[] d_q_accountType = Bytes.toBytes("accountType"); // 账户类型
	public static final byte[] d_q_gender = Bytes.toBytes("gender"); // 性别
	public static final byte[] d_q_age = Bytes.toBytes("age"); // 年龄
	public static final byte[] d_q_roleId = Bytes.toBytes("roleId");// 角色ID
	// public static final byte[] d_q_roleName = Bytes.toBytes("roleName");// 角色名称
	public static final byte[] d_q_roleRace = Bytes.toBytes("roleRace"); // 种族
	public static final byte[] d_q_roleClass = Bytes.toBytes("roleClass"); // 职业
	public static final byte[] d_q_roleLevel = Bytes.toBytes("roleLevel");// 角色等级
	public static final byte[] d_q_createTime = Bytes.toBytes("createTime"); // 创建时间
	public static final byte[] d_q_enableTime = Bytes.toBytes("enableTime"); // 激活时间
	public static final byte[] d_q_deleteTime = Bytes.toBytes("deleteTime"); // 删除时间

	// public static final byte[] d_q_dayLoginTimes = Bytes.toBytes("dayLoginTimes");
	// public static final byte[] d_q_dayOnlineTime = Bytes.toBytes("dayOnlineTime");
	// public static final byte[] d_q_dayPayTimes = Bytes.toBytes("dayPayTimes");
	// public static final byte[] d_q_dayPayAmount = Bytes.toBytes("dayPayAmount");
	// public static final byte[] d_q_dayOlDetail = Bytes.toBytes("dayOlDetail");
	// public static final byte[] d_q_dayPayDetail = Bytes.toBytes("dayPayDetail");
	public static final byte[] d_q_firstLoginDate = Bytes.toBytes("firstLoginDate");
	public static final byte[] d_q_firstLoginTime = Bytes.toBytes("firstLoginTime");
	public static final byte[] d_q_lastLoginDate = Bytes.toBytes("lastLoginDate");
	public static final byte[] d_q_lastLoginTime = Bytes.toBytes("lastLoginTime");
	public static final byte[] d_q_totalOnlineDays = Bytes.toBytes("totalOnlineDays");
	public static final byte[] d_q_totalLoginTimes = Bytes.toBytes("totalLoginTimes");
	public static final byte[] d_q_totalOnlineTime = Bytes.toBytes("totalOnlineTime");
	public static final byte[] d_q_firstPayDate = Bytes.toBytes("firstPayDate");
	public static final byte[] d_q_firstPayTime = Bytes.toBytes("firstPayTime");
	public static final byte[] d_q_firstPayAmount = Bytes.toBytes("firstPayAmount");
	public static final byte[] d_q_lastPayDate = Bytes.toBytes("lastPayDate");
	public static final byte[] d_q_lastPayTime = Bytes.toBytes("lastPayTime");
	public static final byte[] d_q_lastPayAmount = Bytes.toBytes("lastPayAmount");
	public static final byte[] d_q_totalPayAmount = Bytes.toBytes("totalPayAmount");
	public static final byte[] d_q_totalPayTimes = Bytes.toBytes("totalPayTimes");
	public static final byte[] d_q_olTrack = Bytes.toBytes("olTrack");
	public static final byte[] d_q_payTrack = Bytes.toBytes("payTrack");

}
