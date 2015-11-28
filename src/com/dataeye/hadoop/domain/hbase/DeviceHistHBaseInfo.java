package com.dataeye.hadoop.domain.hbase;

import org.apache.hadoop.hbase.util.Bytes;

public class DeviceHistHBaseInfo extends HBaseBasicInfo {

	public static final byte[] device_history_info = Bytes.toBytes("device_history_info");
	public static final byte[] cf_info = Bytes.toBytes("info");

	public static final byte[] s_act_time = Bytes.toBytes("act_time");
	public static final byte[] s_is_real_active = Bytes.toBytes("is_real_act");
	public static final byte[] s_act_and_reg = Bytes.toBytes("is_act_reg");
	public static final byte[] s_acc_set = Bytes.toBytes("account_set");
}
