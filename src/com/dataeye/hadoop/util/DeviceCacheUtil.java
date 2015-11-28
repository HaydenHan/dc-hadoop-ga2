package com.dataeye.hadoop.util;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataeye.hadoop.domain.common.MRConstants;
import com.dataeye.hadoop.domain.hbase.DeviceHistHBaseInfo;
import com.dataeye.hadoop.domain.hbase.DeviceHistoryInfo;
import com.dataeye.hadoop.domain.result.DeviceActiveInfo;

public class DeviceCacheUtil {

	public static boolean isUidExist(String appId, String platform, String uid) throws IOException {
		String rowKey = gettRowKey(appId, platform, uid);
		Get get = new Get(Bytes.toBytes(rowKey));
		return HBaseUtil.checkExist(DeviceHistHBaseInfo.device_history_info, get);
	}

	public static DeviceHistoryInfo getDeviceHistoryInfo(String appId, String platform, String uid) throws IOException {
		String rowKey = gettRowKey(appId, platform, uid);
		Get get = new Get(Bytes.toBytes(rowKey));
		Result result = HBaseUtil.doGet(DeviceHistHBaseInfo.device_history_info, get);
		if (null == result) {
			return null;
		}

		DeviceHistoryInfo info = new DeviceHistoryInfo();
		return info;
	}

	public static void updateDeviceHistoryInfo(DeviceActiveInfo info, int ts) throws IOException {
		// 如果当天新增，则更新静态信息，否则值更新动态信息
		String rowKey = gettRowKey(info.getAppId(), info.getPlatform(), info.getUid());
		Put put = new Put(Bytes.toBytes(rowKey), ts);
		wrapStaticInfo2Put(info, ts, put);

		HBaseUtil.doPut(DeviceHistHBaseInfo.device_history_info, put);
	}

	private static void wrapStaticInfo2Put(DeviceActiveInfo info, int ts, final Put put) {
		if (isValidHBaseValue(info.getAppVer())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_appVer, Bytes.toBytes(info.getAppVer()));
		}
		if (isValidHBaseValue(info.getChannel())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_ch, Bytes.toBytes(info.getChannel()));
		}
		if (isValidHBaseValue(info.getCountry())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_cnty, Bytes.toBytes(info.getCountry()));
		}
		if (isValidHBaseValue(info.getProvince())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_prov, Bytes.toBytes(info.getProvince()));
		}
		if (isValidHBaseValue(info.getCity())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_city, Bytes.toBytes(info.getCity()));
		}
		if (isValidHBaseValue(info.getUid())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_uid, Bytes.toBytes(info.getUid()));
		}
		if (isValidHBaseValue(info.getMac())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_mac, Bytes.toBytes(info.getMac()));
		}
		if (isValidHBaseValue(info.getImei1())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_imei1, Bytes.toBytes(info.getImei1()));
		}
		if (isValidHBaseValue(info.getImsi1())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_imsi1, Bytes.toBytes(info.getImsi1()));
		}
		if (isValidHBaseValue(info.getImei2())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_imei2, Bytes.toBytes(info.getImei2()));
		}
		if (isValidHBaseValue(info.getImsi2())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_imsi2, Bytes.toBytes(info.getImsi2()));
		}
		if (isValidHBaseValue(info.getIdfa())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_idfa, Bytes.toBytes(info.getIdfa()));
		}
		if (isValidHBaseValue(info.getIdfv())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_idfv, Bytes.toBytes(info.getIdfv()));
		}
		if (isValidHBaseValue(info.getModel())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_model, Bytes.toBytes(info.getModel()));
		}
		if (isValidHBaseValue(info.getBrand())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_brand, Bytes.toBytes(info.getBrand()));
		}
		if (isValidHBaseValue(info.getManu())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_manu, Bytes.toBytes(info.getManu()));
		}
		if (isValidHBaseValue(info.getScreen())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_screen, Bytes.toBytes(info.getScreen()));
		}
		if (isValidHBaseValue(info.getCpu())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_cpu, Bytes.toBytes(info.getCpu()));
		}
		if (isValidHBaseValue(info.getRoot())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_root, Bytes.toBytes(info.getRoot()));
		}
		if (isValidHBaseValue(info.getLang())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_lang, Bytes.toBytes(info.getLang()));
		}
		if (isValidHBaseValue(info.getOper())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_oper, Bytes.toBytes(info.getOper()));
		}
		if (isValidHBaseValue(info.getOperISO())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_operISO,
					Bytes.toBytes(info.getOperISO()));
		}
		if (isValidHBaseValue(info.getOs())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_os, Bytes.toBytes(info.getOs()));
		}
		if (isValidHBaseValue(info.getTimeZone())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_tz, Bytes.toBytes(info.getTimeZone()));
		}
		if (isValidHBaseValue(info.getIp())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_ip, Bytes.toBytes(info.getIp()));
		}
		if (isValidHBaseValue(info.getNet())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_net, Bytes.toBytes(info.getNet()));
		}
		if (isValidHBaseValue(info.getLongitude())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_lon, Bytes.toBytes(info.getLongitude()));
		}
		if (isValidHBaseValue(info.getLatitude())) {
			put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_q_lat, Bytes.toBytes(info.getLatitude()));
		}

		put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_act_time, Bytes.toBytes(info.getActiveTime()));

		put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_is_real_active,
				Bytes.toBytes(info.getIsRealActive()));

		put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_act_and_reg,
				Bytes.toBytes(info.getIsActAndReg()));

		put.addColumn(DeviceHistHBaseInfo.cf_info, DeviceHistHBaseInfo.s_acc_set, Bytes.toBytes(info.getAccIdSet()));
	}

	private static boolean isValidHBaseValue(String hbaseValue) {
		return !StringUtil.isEmpty(hbaseValue) && !MRConstants.STR_PLACE_HOLDER.equals(hbaseValue);
	}

	private static String gettRowKey(String appId, String platform, String uid) {
		String row = appId + DeviceHistHBaseInfo.SEPARATOR + platform + DeviceHistHBaseInfo.SEPARATOR + uid;
		return row;
	}
}
