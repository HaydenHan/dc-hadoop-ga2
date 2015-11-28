package com.dataeye.hadoop.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataeye.hadoop.domain.common.AccountBaseWritable;
import com.dataeye.hadoop.domain.common.MRConstants;
import com.dataeye.hadoop.domain.hbase.AccountExtHBaseInfo;
import com.dataeye.hadoop.domain.hbase.AccountHistHBaseInfo;
import com.dataeye.hadoop.domain.hbase.AccountHistoryInfo;
import com.dataeye.hadoop.domain.hbase.HBaseBasicInfo;
import com.dataeye.hadoop.domain.kv.AccountRollInfo;

public class AccountCacheUtil {

	public static void getAndSetHistoryInfo(final AccountRollInfo accRollInfo, int timestamp) throws IOException {
		String rowKey = getAccountRowKey(accRollInfo.getAppId(), accRollInfo.getPlatform(),
				accRollInfo.getGameServer(), accRollInfo.getAccountId());
		Get get = new Get(Bytes.toBytes(rowKey));
		get.setTimeRange(0, timestamp + 1);
		get.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_createTime);
		get.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_firstLoginDate);
		get.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_firstLoginTime);
		get.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_lastLoginDate);
		get.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_lastLoginTime);
		get.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_totalOnlineDays);
		get.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_totalLoginTimes);
		get.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_totalOnlineTime);
		get.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_firstPayDate);
		get.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_firstPayTime);
		get.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_lastPayDate);
		get.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_lastPayTime);
		get.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_lastPayAmount);
		get.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_totalPayTimes);
		get.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_totalPayAmount);
		get.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_olTrack);
		get.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_payTrack);
		Result result = HBaseUtil.doGet(AccountHistHBaseInfo.account_history_info, get);
		if (null != result) {
			wrapResult2DynamicInfo(accRollInfo, result);
		}
	}

	public static void getAndSetFromGS(final AccountBaseWritable accInfo, int timestamp) throws IOException {

		if (MRConstants.ALL_GS.equals(accInfo.getGameServer())) {
			accInfo.setFromServer(MRConstants.STR_PLACE_HOLDER);
			return;
		}

		String rowKey = getAccountExtRowKey(accInfo.getAppId(), accInfo.getPlatform(), accInfo.getAccountId());
		Get get = new Get(Bytes.toBytes(rowKey));
		get.setTimeRange(0, timestamp + 1);
		get.addFamily(AccountExtHBaseInfo.cf_gs);
		Result result = HBaseUtil.doGet(AccountExtHBaseInfo.account_ext_info, get);

		if (result != null) {
			int minTime = Integer.MAX_VALUE;
			NavigableMap<byte[], byte[]> hbaseMap = result.getFamilyMap(AccountExtHBaseInfo.cf_gs);
			if (hbaseMap != null && !hbaseMap.isEmpty()) {
				for (Entry<byte[], byte[]> entry : hbaseMap.entrySet()) {
					String gameServer = Bytes.toString(entry.getKey());
					int firstLoginDate = Bytes.toInt(entry.getValue());
					if (firstLoginDate < minTime && !MRConstants.ALL_GS.equals(gameServer)) {
						accInfo.setFromServer(gameServer);
					}
				}
			}
		}
	}

	public static String getFirstLoginGS(String appId, String platform, String accountId, long timestamp)
			throws IOException {
		Map<String, Integer> gsMap = getExtGameserverInfo(appId, platform, accountId, timestamp);
		if (null == gsMap) {
			return null;
		}

		int minTime = Integer.MAX_VALUE;
		String firstLoginGS = MRConstants.STR_PLACE_HOLDER;
		for (Entry<String, Integer> entry : gsMap.entrySet()) {
			if (entry.getValue() < minTime && !MRConstants.ALL_GS.equals(entry.getKey())) {
				minTime = entry.getValue();
				firstLoginGS = entry.getKey();
			}
		}
		return firstLoginGS;
	}

	public static Map<String, Integer> getExtGameserverInfo(String appId, String platform, String accountId,
			long timestamp) throws IOException {
		String rowKey = getAccountExtRowKey(appId, platform, accountId);
		Get get = new Get(Bytes.toBytes(rowKey));
		get.setTimeRange(0, timestamp + 1);
		get.addFamily(AccountExtHBaseInfo.cf_gs);
		Result result = HBaseUtil.doGet(AccountExtHBaseInfo.account_ext_info, get);
		if (null == result) {
			return null;
		}

		Map<String, Integer> gsMap = new HashMap<String, Integer>();
		NavigableMap<byte[], byte[]> hbaseMap = result.getFamilyMap(AccountExtHBaseInfo.cf_gs);
		if (hbaseMap != null && !hbaseMap.isEmpty()) {
			for (Entry<byte[], byte[]> entry : hbaseMap.entrySet()) {
				gsMap.put(Bytes.toString(entry.getKey()), Bytes.toInt(entry.getValue()));
			}
		}

		return gsMap;
	}

	public static void updateAccountHistoryInfo(AccountRollInfo rollInfo, int ts) throws IOException {
		// 如果当天新增，则更新静态信息，否则值更新动态信息
		String rowKey = getAccountRowKey(rollInfo);
		Put put = new Put(Bytes.toBytes(rowKey), ts);

		if (!rollInfo.getUid().equals(rollInfo.getHistoryUid())) {
			wrapStaticInfo2Put(rollInfo, ts, put);
		}
		wrapDynamicInfo2Put(rollInfo, ts, put);

		HBaseUtil.doPut(AccountHistHBaseInfo.account_history_info, put);
	}

	public static void updateAccountExtInfo(AccountRollInfo rollInfo, int ts) throws IOException {
		// 如果当天新增，则更新静态信息，否则值更新动态信息
		String rowKey = getAccountExtRowKey(rollInfo);
		Put put = new Put(Bytes.toBytes(rowKey), ts);

		// 在某区服新增
		if (rollInfo.getDayCreateTime() > 0 && 0 == rollInfo.getFirstLoginDate()) {
			// put.addColumn(AccountExtHBaseInfo.cf_gs, Bytes.toBytes(rollInfo.getGameServer()), Bytes.toBytes(ts));
			put.addColumn(AccountExtHBaseInfo.cf_gs, Bytes.toBytes(rollInfo.getGameServer()), Bytes.toBytes(ts));
		}

		if (!rollInfo.getUid().equals(rollInfo.getHistoryUid())) {
			// put.addColumn(AccountExtHBaseInfo.cf_uid, Bytes.toBytes(rollInfo.getUid()), Bytes.toBytes(ts));
			put.addColumn(AccountExtHBaseInfo.cf_uid, Bytes.toBytes(rollInfo.getUid()), Bytes.toBytes(ts));
		}
		HBaseUtil.doPut(AccountHistHBaseInfo.account_history_info, put);
	}

	public static AccountHistoryInfo wrapAccountHistoryInfo(Result result) throws IOException {
		if (null == result) {
			return null;
		}

		AccountHistoryInfo info = new AccountHistoryInfo();
		wrapResult2StaticInfo(info, result);
		wrapResult2DynamicInfo(info, result);

		return info;
	}

	private static void wrapStaticInfo2Put(AccountRollInfo rollInfo, int ts, final Put put) {
		put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_time_stamp,
				Bytes.toBytes(rollInfo.getTimestamp()));
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getAppId())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_appId,
					Bytes.toBytes(rollInfo.getAppId()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getAppVer())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_appVer,
					Bytes.toBytes(rollInfo.getAppVer()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getPlatform())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_plat,
					Bytes.toBytes(rollInfo.getPlatform()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getChannel())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_ch,
					Bytes.toBytes(rollInfo.getChannel()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getCountry())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_cnty,
					Bytes.toBytes(rollInfo.getCountry()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getProvince())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_prov,
					Bytes.toBytes(rollInfo.getProvince()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getCity())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_city,
					Bytes.toBytes(rollInfo.getCity()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getUid())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_uid,
					Bytes.toBytes(rollInfo.getUid()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getMac())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_mac,
					Bytes.toBytes(rollInfo.getMac()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getImei1())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_imei1,
					Bytes.toBytes(rollInfo.getImei1()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getImsi1())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_imsi1,
					Bytes.toBytes(rollInfo.getImsi1()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getImei2())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_imei2,
					Bytes.toBytes(rollInfo.getImei2()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getImsi2())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_imsi2,
					Bytes.toBytes(rollInfo.getImsi2()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getIdfa())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_idfa,
					Bytes.toBytes(rollInfo.getIdfa()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getIdfv())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_idfv,
					Bytes.toBytes(rollInfo.getIdfv()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getModel())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_model,
					Bytes.toBytes(rollInfo.getModel()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getBrand())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_brand,
					Bytes.toBytes(rollInfo.getBrand()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getManu())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_manu,
					Bytes.toBytes(rollInfo.getManu()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getScreen())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_screen,
					Bytes.toBytes(rollInfo.getScreen()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getCpu())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_cpu,
					Bytes.toBytes(rollInfo.getCpu()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getRoot())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_root,
					Bytes.toBytes(rollInfo.getRoot()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getLang())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_lang,
					Bytes.toBytes(rollInfo.getLang()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getOper())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_oper,
					Bytes.toBytes(rollInfo.getOper()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getOperISO())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_operISO,
					Bytes.toBytes(rollInfo.getOperISO()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getOs())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_os, Bytes.toBytes(rollInfo.getOs()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getTimeZone())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_tz,
					Bytes.toBytes(rollInfo.getTimeZone()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getIp())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_ip, Bytes.toBytes(rollInfo.getIp()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getNet())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_net,
					Bytes.toBytes(rollInfo.getNet()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getLongitude())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_lon,
					Bytes.toBytes(rollInfo.getLongitude()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getLatitude())) {
			put.addColumn(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_lat,
					Bytes.toBytes(rollInfo.getLatitude()));
		}
	}

	private static void wrapDynamicInfo2Put(AccountRollInfo info, int ts, final Put put) {

		if (HBaseBasicInfo.isValidHBaseValue(info.getGameServer())) {
			put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_gs,
					Bytes.toBytes(info.getGameServer()));
		}

		if (HBaseBasicInfo.isValidHBaseValue(info.getFromServer())) {
			put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_fgs,
					Bytes.toBytes(info.getFromServer()));
		}

		if (HBaseBasicInfo.isValidHBaseValue(info.getAccountId())) {
			put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_account_id,
					Bytes.toBytes(info.getAccountId()));
		}

		if (HBaseBasicInfo.isValidHBaseValue(info.getAccountType())) {
			put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_account_type,
					Bytes.toBytes(info.getAccountType()));
		}

		if (HBaseBasicInfo.isValidHBaseValue(info.getGender())) {
			put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_gender,
					Bytes.toBytes(info.getGender()));
		}

		if (HBaseBasicInfo.isValidHBaseValue(info.getAge())) {
			put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_age, Bytes.toBytes(info.getAge()));
		}

		int firstLogInDate = info.getFirstLoginDate();
		if (info.getDayCreateTime() > 0 && 0 == info.getFirstLoginDate()) {
			put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_createTime, Bytes.toBytes(ts));
		}
		if (0 == info.getFirstLoginDate() && info.getDayLoginTimes() > 0) {
			put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_firstLoginDate, Bytes.toBytes(ts));
			firstLogInDate = ts;
		}

		if (info.getDayLoginTimes() > 0) {
			put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_lastLoginDate, Bytes.toBytes(ts));
			put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_totalOnlineDays,
					Bytes.toBytes(info.getTotalOnlineDays() + 1));
			put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_totalLoginTimes,
					Bytes.toBytes(info.getTotalLoginTimes() + info.getDayLoginTimes()));
			put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_totalOnlineTime,
					Bytes.toBytes(info.getTotalOnlineTime() + info.getDayOnlineTime()));

			int firstLoginTime = Integer.MAX_VALUE;
			int lastLoginTime = 0;

			Map<Integer, Integer> olMap = DCJsonUtil.jsonToIntegerMap(info.getDayOlDetail());
			for (Integer time : olMap.keySet()) {
				firstLoginTime = Math.min(firstLoginTime, time);
				lastLoginTime = Math.max(lastLoginTime, time);
			}

			// 新增当天设置首登时间
			if (0 == info.getFirstLoginDate()) {
				put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_firstLoginTime,
						Bytes.toBytes(firstLoginTime));
			}

			put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_lastLoginTime,
					Bytes.toBytes(lastLoginTime));

			String newOLTrack = TrackUtil.addTargetDate2Track(info.getOlTrack(), firstLogInDate, ts);
			put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_olTrack, Bytes.toBytes(newOLTrack));
		}

		if (info.getDayPayTimes() > 0) {
			int firstPayDate = info.getFirstPayDate();
			if (0 == info.getFirstPayDate()) {
				put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_firstPayDate, Bytes.toBytes(ts));
				firstPayDate = ts;
			}
			put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_lastPayDate, Bytes.toBytes(ts));
			put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_totalPayAmount,
					Bytes.toBytes(info.getTotalPayAmount() + info.getDayPayAmount()));
			put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_totalPayTimes,
					Bytes.toBytes(info.getTotalPayTimes() + info.getDayPayTimes()));

			int firstPayTime = Integer.MAX_VALUE;
			int lastPayTime = 0;

			Map<Integer, Float> payMap = DCJsonUtil.jsonToIntFloatMap(info.getDayPayDetail());
			for (Integer time : payMap.keySet()) {
				firstPayTime = Math.min(firstPayTime, time);
				lastPayTime = Math.max(lastPayTime, time);
			}
			float lastPayAmount = payMap.get(lastPayTime);

			// 首付当天设置第一次付费时间
			if (0 == info.getFirstPayDate()) {
				put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_firstPayTime,
						Bytes.toBytes(firstPayTime));
			}

			put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_lastPayTime,
					Bytes.toBytes(lastPayTime));
			put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_lastPayAmount,
					Bytes.toBytes(lastPayAmount));

			String newPayTrack = TrackUtil.addTargetDate2Track(info.getPayTrack(), firstPayDate, ts);
			put.addColumn(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_payTrack,
					Bytes.toBytes(newPayTrack));
		}
	}

	private static void wrapResult2StaticInfo(AccountHistoryInfo info, Result result) {

		byte[] timeStamp = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_time_stamp);
		if (null != timeStamp) {
			info.setTimestamp(Bytes.toInt(timeStamp));
		}
		byte[] appID = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_appId);
		if (null != appID) {
			info.setAppId(Bytes.toString(appID));
		}

		byte[] appVer = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_appVer);
		if (null != appVer) {
			info.setAppVer(Bytes.toString(appVer));
		}

		byte[] platForm = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_plat);
		if (null != platForm) {
			info.setPlatform(Bytes.toString(platForm));
		}

		byte[] channel = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_ch);
		if (null != channel) {
			info.setChannel(Bytes.toString(channel));
		}

		byte[] country = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_cnty);
		if (null != country) {
			info.setCountry(Bytes.toString(country));
		}

		byte[] province = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_prov);
		if (null != province) {
			info.setProvince(Bytes.toString(province));
		}

		byte[] city = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_city);
		if (null != city) {
			info.setCity(Bytes.toString(city));
		}

		byte[] uid = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_uid);
		if (null != uid) {
			info.setUid(Bytes.toString(uid));
		}

		byte[] mac = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_mac);
		if (null != mac) {
			info.setMac(Bytes.toString(mac));
		}

		byte[] imei1 = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_imei1);
		if (null != imei1) {
			info.setImei1(Bytes.toString(imei1));
		}

		byte[] imsi1 = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_imsi1);
		if (null != imsi1) {
			info.setImsi1(Bytes.toString(imsi1));
		}

		byte[] imei2 = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_imei2);
		if (null != imei2) {
			info.setImei2(Bytes.toString(imei2));
		}

		byte[] imsi2 = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_imsi2);
		if (null != imsi2) {
			info.setImsi2(Bytes.toString(imsi2));
		}

		byte[] idfa = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_idfa);
		if (null != idfa) {
			info.setIdfa(Bytes.toString(idfa));
		}

		byte[] idfv = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_idfv);
		if (null != idfv) {
			info.setIdfv(Bytes.toString(idfv));
		}

		byte[] model = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_model);
		if (null != model) {
			info.setModel(Bytes.toString(model));
		}

		byte[] brand = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_brand);
		if (null != brand) {
			info.setBrand(Bytes.toString(brand));
		}

		byte[] manu = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_manu);
		if (null != manu) {
			info.setManu(Bytes.toString(manu));
		}

		byte[] screen = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_screen);
		if (null != screen) {
			info.setScreen(Bytes.toString(screen));
		}

		byte[] cpu = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_cpu);
		if (null != cpu) {
			info.setCpu(Bytes.toString(cpu));
		}

		byte[] root = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_root);
		if (null != root) {
			info.setRoot(Bytes.toString(root));
		}

		byte[] language = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_lang);
		if (null != language) {
			info.setLang(Bytes.toString(language));
		}

		byte[] oper = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_oper);
		if (null != oper) {
			info.setOper(Bytes.toString(oper));
		}

		byte[] operISO = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_operISO);
		if (null != operISO) {
			info.setOperISO(Bytes.toString(operISO));
		}

		byte[] os = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_os);
		if (null != os) {
			info.setOs(Bytes.toString(os));
		}

		byte[] timezone = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_tz);
		if (null != timezone) {
			info.setTimeZone(Bytes.toString(timezone));
		}

		byte[] ip = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_ip);
		if (null != ip) {
			info.setIp(Bytes.toString(ip));
		}

		byte[] net = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_net);
		if (null != net) {
			info.setNet(Bytes.toString(net));
		}

		byte[] longitude = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_lon);
		if (null != longitude) {
			info.setLongitude(Bytes.toString(longitude));
		}

		byte[] latitude = result.getValue(AccountHistHBaseInfo.cf_static, AccountHistHBaseInfo.s_q_lat);
		if (null != latitude) {
			info.setLatitude(Bytes.toString(latitude));
		}
	}

	private static void wrapResult2DynamicInfo(AccountHistoryInfo info, Result result) {
		byte[] gs = result.getValue(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_gs);
		if (null != gs) {
			info.setGameServer(Bytes.toString(gs));
		}

		byte[] fgs = result.getValue(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_fgs);
		if (null != fgs) {
			info.setFromServer(Bytes.toString(fgs));
		}

		byte[] accId = result.getValue(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_account_id);
		if (null != accId) {
			info.setAccountId(Bytes.toString(accId));
		}

		byte[] accType = result.getValue(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_account_type);
		if (null != accType) {
			info.setAccountType(Bytes.toString(accType));
		}

		byte[] gender = result.getValue(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_gender);
		if (null != gender) {
			info.setGender(Bytes.toString(gender));
		}

		byte[] age = result.getValue(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_age);
		if (null != age) {
			info.setAge(Bytes.toString(age));
		}

		byte[] createTime = result.getValue(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_createTime);
		if (null != createTime) {
			info.setCreateTime(Bytes.toInt(createTime));
		}
		// firstLoginDate
		byte[] firstLoginDate = result.getValue(AccountHistHBaseInfo.cf_dynamic,
				AccountHistHBaseInfo.d_q_firstLoginDate);
		if (null != firstLoginDate) {
			info.setFirstLoginDate(Bytes.toInt(firstLoginDate));
		}
		// firstLoginTime
		byte[] firstLoginTime = result.getValue(AccountHistHBaseInfo.cf_dynamic,
				AccountHistHBaseInfo.d_q_firstLoginTime);
		if (null != firstLoginTime) {
			info.setFirstLoginTime(Bytes.toInt(firstLoginTime));
		}
		// lastLoginDate
		byte[] lastLoginDate = result.getValue(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_lastLoginDate);
		if (null != lastLoginDate) {
			info.setLastLoginDate(Bytes.toInt(lastLoginDate));
		}
		// lastLoginTime
		byte[] lastLoginTime = result.getValue(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_lastLoginTime);
		if (null != lastLoginTime) {
			info.setLastLoginTime(Bytes.toInt(lastLoginTime));
		}
		// totalOnlineDays
		byte[] totalOnlineDays = result.getValue(AccountHistHBaseInfo.cf_dynamic,
				AccountHistHBaseInfo.d_q_totalOnlineDays);
		if (null != totalOnlineDays) {
			info.setTotalOnlineDays(Bytes.toInt(totalOnlineDays));
		}
		// totalLoginTimes
		byte[] totalLoginTimes = result.getValue(AccountHistHBaseInfo.cf_dynamic,
				AccountHistHBaseInfo.d_q_totalLoginTimes);
		if (null != totalLoginTimes) {
			info.setTotalLoginTimes(Bytes.toInt(totalLoginTimes));
		}
		// totalOnlineTime
		byte[] totalOnlineTime = result.getValue(AccountHistHBaseInfo.cf_dynamic,
				AccountHistHBaseInfo.d_q_totalOnlineTime);
		if (null != totalOnlineTime) {
			info.setTotalOnlineTime(Bytes.toInt(totalOnlineTime));
		}
		// firstPayDate
		byte[] firstPayDate = result.getValue(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_firstPayDate);
		if (null != firstPayDate) {
			info.setFirstPayDate(Bytes.toInt(firstPayDate));
		}
		// firstPayTime
		byte[] firstPayTime = result.getValue(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_firstPayTime);
		if (null != firstPayTime) {
			info.setFirstPayTime(Bytes.toInt(firstPayTime));
		}
		// lastPayDate
		byte[] lastPayDate = result.getValue(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_lastPayDate);
		if (null != lastPayDate) {
			info.setLastPayDate(Bytes.toInt(lastPayDate));
		}
		// lastPayTime
		byte[] lastPayTime = result.getValue(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_lastPayTime);
		if (null != lastPayTime) {
			info.setLastPayTime(Bytes.toInt(lastPayTime));
		}
		// lastPayAmount
		byte[] lastPayAmount = result.getValue(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_lastPayAmount);
		if (null != lastPayAmount) {
			info.setLastPayAmount(Bytes.toFloat(lastPayAmount));
		}
		// totalPayAmount
		byte[] totalPayAmount = result.getValue(AccountHistHBaseInfo.cf_dynamic,
				AccountHistHBaseInfo.d_q_totalPayAmount);
		if (null != totalPayAmount) {
			info.setTotalPayAmount(Bytes.toFloat(totalPayAmount));
		}
		// totalPayTimes
		byte[] totalPayTimes = result.getValue(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_totalPayTimes);
		if (null != totalPayTimes) {
			info.setTotalPayTimes(Bytes.toInt(totalPayTimes));
		}
		// olTrack
		byte[] olTrack = result.getValue(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_olTrack);
		if (null != olTrack) {
			info.setOlTrack(Bytes.toString(olTrack));
		}
		// payTrack
		byte[] payTrack = result.getValue(AccountHistHBaseInfo.cf_dynamic, AccountHistHBaseInfo.d_q_payTrack);
		if (null != payTrack) {
			info.setPayTrack(Bytes.toString(payTrack));
		}
	}

	private static String getAccountRowKey(AccountRollInfo rollInfo) {
		return getAccountRowKey(rollInfo.getAppId(), rollInfo.getPlatform(), rollInfo.getGameServer(),
				rollInfo.getAccountId());
	}

	private static String getAccountExtRowKey(AccountRollInfo rollInfo) {
		return getAccountExtRowKey(rollInfo.getAppId(), rollInfo.getPlatform(), rollInfo.getAccountId());
	}

	private static String getAccountRowKey(String appId, String platform, String gameServer, String accountId) {
		String row = appId + AccountHistHBaseInfo.SEPARATOR + platform + AccountHistHBaseInfo.SEPARATOR + gameServer
				+ AccountHistHBaseInfo.SEPARATOR + accountId;
		return row;
	}

	private static String getAccountExtRowKey(String appId, String platform, String accountId) {
		String row = appId + AccountHistHBaseInfo.SEPARATOR + platform + AccountHistHBaseInfo.SEPARATOR + accountId;
		return row;
	}

}
