package com.dataeye.hadoop.domain.hbase;

import org.apache.hadoop.hbase.util.Bytes;

import com.dataeye.hadoop.domain.common.MRConstants;
import com.dataeye.hadoop.util.StringUtil;

public class HBaseBasicInfo {

	public static final String SEPARATOR = "|";

	public static final byte[] s_time_stamp = Bytes.toBytes("timeStamp");
	public static final byte[] s_q_appId = Bytes.toBytes("appId");
	public static final byte[] s_q_appVer = Bytes.toBytes("appVer");
	public static final byte[] s_q_plat = Bytes.toBytes("plat");
	public static final byte[] s_q_ch = Bytes.toBytes("ch");
	public static final byte[] s_q_cnty = Bytes.toBytes("cnty");
	public static final byte[] s_q_prov = Bytes.toBytes("prov");
	public static final byte[] s_q_city = Bytes.toBytes("city");
	public static final byte[] s_q_uid = Bytes.toBytes("uid");
	public static final byte[] s_q_mac = Bytes.toBytes("mac");
	public static final byte[] s_q_imei1 = Bytes.toBytes("imei1");
	public static final byte[] s_q_imsi1 = Bytes.toBytes("imsi1");
	public static final byte[] s_q_imei2 = Bytes.toBytes("imei2");
	public static final byte[] s_q_imsi2 = Bytes.toBytes("imsi2");
	public static final byte[] s_q_idfa = Bytes.toBytes("idfa");
	public static final byte[] s_q_idfv = Bytes.toBytes("idfv");
	public static final byte[] s_q_model = Bytes.toBytes("model");
	public static final byte[] s_q_brand = Bytes.toBytes("brand");
	public static final byte[] s_q_manu = Bytes.toBytes("manu");
	public static final byte[] s_q_screen = Bytes.toBytes("screen");
	public static final byte[] s_q_cpu = Bytes.toBytes("cpu");
	public static final byte[] s_q_root = Bytes.toBytes("root");
	public static final byte[] s_q_lang = Bytes.toBytes("lang");
	public static final byte[] s_q_oper = Bytes.toBytes("oper");
	public static final byte[] s_q_operISO = Bytes.toBytes("operISO");
	public static final byte[] s_q_os = Bytes.toBytes("os");
	public static final byte[] s_q_tz = Bytes.toBytes("tz");
	public static final byte[] s_q_ip = Bytes.toBytes("ip");
	public static final byte[] s_q_net = Bytes.toBytes("net");
	public static final byte[] s_q_lon = Bytes.toBytes("lon");
	public static final byte[] s_q_lat = Bytes.toBytes("lat");

	public static boolean isValidHBaseValue(String hbaseValue) {
		return !StringUtil.isEmpty(hbaseValue) && !MRConstants.STR_PLACE_HOLDER.equals(hbaseValue);
	}
}
