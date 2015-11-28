package com.dataeye.hadoop.util;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.dataeye.hadoop.domain.common.MRConstants;
import com.dataeye.hadoop.domain.hbase.HBaseBasicInfo;
import com.dataeye.hadoop.domain.hbase.RoleHistHBaseInfo;
import com.dataeye.hadoop.domain.hbase.RoleHistoryInfo;
import com.dataeye.hadoop.domain.kv.role.RoleRollInfo;

public class RoleCacheUtil {

	/**
	 * <pre>
	 * 根据hbase结果组装实体类
	 * @param result
	 * @return
	 * @throws IOException
	 * @author Hayden<br>
	 * @date 2015年11月6日 下午8:59:32
	 * <br>
	 */
	public static RoleHistoryInfo wrapRoleHistoryInfo(Result result) throws IOException {
		if (null == result) {
			return null;
		}

		RoleHistoryInfo info = new RoleHistoryInfo();
		wrapResult2StaticInfo(info, result);
		wrapResult2DynamicInfo(info, result);

		return info;
	}

	public static void updateRoleHistoryInfo(RoleRollInfo rollInfo, int ts) throws IOException {
		// 如果角色为当天新增，则更新静态信息，否则值更新动态信息
		String rowKey = getRoleRowKey(rollInfo.getAppId(), rollInfo.getPlatform(), rollInfo.getGameServer(),
				rollInfo.getAccountId(), rollInfo.getRoleId());
		Put put = new Put(Bytes.toBytes(rowKey), ts);
		// 如果是新增角色，更新静态信息
		if (rollInfo.getFirstLoginDate() == 0) {
			wrapStaticInfo2Put(rollInfo, ts, put);
		}
		wrapDynamicInfo2Put(rollInfo, ts, put);

		HBaseUtil.doPut(RoleHistHBaseInfo.role_history_info, put);
	}

	/**
	 * <pre>
	 * 获取rowKey
	 * @param appId
	 * @param platform
	 * @param gameServer
	 * @param accountId
	 * @param roleId
	 * @return
	 * @author Hayden<br>
	 * @date 2015年11月6日 下午9:00:01
	 * <br>
	 */
	private static String getRoleRowKey(String appId, String platform, String gameServer, String accountId,
			String roleId) {
		String row = appId + RoleHistHBaseInfo.SEPARATOR + platform + RoleHistHBaseInfo.SEPARATOR + gameServer
				+ RoleHistHBaseInfo.SEPARATOR + accountId + RoleHistHBaseInfo.SEPARATOR + roleId;
		return row;
	}

	/**
	 * <pre>
	 * 判断roleId是否已经存在
	 * @param appId
	 * @param platform
	 * @param uid
	 * @return
	 * @throws IOException
	 * @author Hayden<br>
	 * @date 2015年11月7日 上午10:06:17
	 * <br>
	 */
	public static boolean isRoleExist(String appId, String platform, String gameServer, String accountId, String roleId)
			throws IOException {
		String rowKey = getRoleRowKey(appId, platform, gameServer, accountId, roleId);
		Get get = new Get(Bytes.toBytes(rowKey));
		return HBaseUtil.checkExist(RoleHistHBaseInfo.role_history_info, get);
	}

	/**
	 * <pre>
	 * 查询角色缓存信息
	 * @param appId
	 * @param platform
	 * @param gameServer
	 * @param accountId
	 * @param timestamp
	 * @return
	 * @throws IOException
	 * @author Hayden<br>
	 * @date 2015年11月5日 下午9:52:05
	 * <br>
	 */
	public static RoleHistoryInfo getRoleHistoryInfo(String appId, String platform, String gameServer,
			String accountId, String roleId, long timestamp) throws IOException {
		String rowKey = getRoleRowKey(appId, platform, gameServer, accountId, roleId);
		Get get = new Get(Bytes.toBytes(rowKey));
		get.setTimeRange(0, timestamp + 1);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_gameServer);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_fromServer);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_accountId);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_accountType);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_gender);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_age);

		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_roleId);
		// get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_roleName);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_roleRace);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_roleClass);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_roleLevel);

		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_createTime);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_enableTime);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_deleteTime);

		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_firstLoginDate);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_firstLoginTime);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_lastLoginDate);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_lastLoginTime);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_totalOnlineDays);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_totalLoginTimes);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_totalOnlineTime);

		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_firstPayDate);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_firstPayTime);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_firstPayAmount);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_lastPayDate);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_lastPayTime);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_lastPayAmount);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_totalPayAmount);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_totalPayTimes);

		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_olTrack);
		get.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_payTrack);

		RoleHistoryInfo info = new RoleHistoryInfo();
		Result result = HBaseUtil.doGet(RoleHistHBaseInfo.role_history_info, get);
		if (null == result) {
			return info;
		}
		wrapResult2DynamicInfo(info, result);
		return info;
	}

	/**
	 * <pre>
	 * 更新habse静态字段
	 * @param rollInfo
	 * @param ts
	 * @param put
	 * @author Hayden<br>
	 * @date 2015年11月6日 下午9:08:36
	 * <br>
	 */
	private static void wrapStaticInfo2Put(RoleRollInfo rollInfo, int ts, final Put put) {
		put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_time_stamp,
				Bytes.toBytes(rollInfo.getTimestamp()));
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getAppId())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_appId, Bytes.toBytes(rollInfo.getAppId()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getAppVer())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_appVer,
					Bytes.toBytes(rollInfo.getAppVer()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getPlatform())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_plat,
					Bytes.toBytes(rollInfo.getPlatform()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getChannel())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_ch, Bytes.toBytes(rollInfo.getChannel()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getCountry())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_cnty, Bytes.toBytes(rollInfo.getCountry()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getProvince())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_prov,
					Bytes.toBytes(rollInfo.getProvince()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getCity())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_city, Bytes.toBytes(rollInfo.getCity()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getUid())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_uid, Bytes.toBytes(rollInfo.getUid()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getMac())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_mac, Bytes.toBytes(rollInfo.getMac()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getImei1())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_imei1, Bytes.toBytes(rollInfo.getImei1()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getImsi1())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_imsi1, Bytes.toBytes(rollInfo.getImsi1()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getImei2())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_imei2, Bytes.toBytes(rollInfo.getImei2()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getImsi2())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_imsi2, Bytes.toBytes(rollInfo.getImsi2()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getIdfa())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_idfa, Bytes.toBytes(rollInfo.getIdfa()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getIdfv())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_idfv, Bytes.toBytes(rollInfo.getIdfv()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getModel())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_model, Bytes.toBytes(rollInfo.getModel()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getBrand())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_brand, Bytes.toBytes(rollInfo.getBrand()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getManu())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_manu, Bytes.toBytes(rollInfo.getManu()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getScreen())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_screen,
					Bytes.toBytes(rollInfo.getScreen()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getCpu())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_cpu, Bytes.toBytes(rollInfo.getCpu()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getRoot())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_root, Bytes.toBytes(rollInfo.getRoot()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getLang())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_lang, Bytes.toBytes(rollInfo.getLang()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getOper())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_oper, Bytes.toBytes(rollInfo.getOper()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getOperISO())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_operISO,
					Bytes.toBytes(rollInfo.getOperISO()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getOs())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_os, Bytes.toBytes(rollInfo.getOs()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getTimeZone())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_tz, Bytes.toBytes(rollInfo.getTimeZone()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getIp())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_ip, Bytes.toBytes(rollInfo.getIp()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getNet())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_net, Bytes.toBytes(rollInfo.getNet()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getLongitude())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_lon,
					Bytes.toBytes(rollInfo.getLongitude()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(rollInfo.getLatitude())) {
			put.addColumn(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_lat, Bytes.toBytes(rollInfo.getLatitude()));
		}
	}

	private static void wrapDynamicInfo2Put(RoleRollInfo info, int ts, final Put put) {

		if (HBaseBasicInfo.isValidHBaseValue(info.getGameServer())) {
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_gameServer,
					Bytes.toBytes(info.getGameServer()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(info.getFromServer())) {
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_fromServer,
					Bytes.toBytes(info.getFromServer()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(info.getAccountId())) {
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_accountId,
					Bytes.toBytes(info.getAccountId()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(info.getAccountType())) {
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_accountType,
					Bytes.toBytes(info.getAccountType()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(info.getGender())) {
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_gender, Bytes.toBytes(info.getGender()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(info.getAge())) {
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_age, Bytes.toBytes(info.getAge()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(info.getRoleId())) {
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_roleId, Bytes.toBytes(info.getRoleId()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(info.getRoleRace())) {
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_roleRace,
					Bytes.toBytes(info.getRoleRace()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(info.getRoleClass())) {
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_roleClass,
					Bytes.toBytes(info.getRoleClass()));
		}
		if (HBaseBasicInfo.isValidHBaseValue(info.getRoleLevel())) {
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_roleLevel,
					Bytes.toBytes(info.getRoleLevel()));
		}
		// 创建时间
		if (MRConstants.INT_TRUE == info.getIsNewCreate()) {
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_createTime, Bytes.toBytes(ts));
		}
		if (info.getEnableTime() > 0) {
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_enableTime,
					Bytes.toBytes(info.getEnableTime()));
		}
		if (info.getDeleteTime() > 0) {
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_deleteTime,
					Bytes.toBytes(info.getDeleteTime()));
		}

		// 首登
		int firstLogInDate = info.getFirstLoginDate();
		if (0 == info.getFirstLoginDate() && info.getDayLoginTimes() > 0) {
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_firstLoginDate, Bytes.toBytes(ts));
			firstLogInDate = ts;
		}

		// Integer tokenType = new Integer(0);

		if (info.getDayLoginTimes() > 0) {
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_lastLoginDate, Bytes.toBytes(ts));
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_totalOnlineDays,
					Bytes.toBytes(info.getTotalOnlineDays() + 1));
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_totalLoginTimes,
					Bytes.toBytes(info.getTotalLoginTimes() + info.getDayLoginTimes()));
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_totalOnlineTime,
					Bytes.toBytes(info.getTotalOnlineTime() + info.getDayOnlineTime()));

			int firstLoginTime = Integer.MAX_VALUE;
			int lastLoginTime = 0;

			// Map<Integer, Integer> olMap = DCJsonUtil.getMapFromJson(info.getDayOlDetail(), tokenType, tokenType);
			Map<Integer, Integer> olMap = DCJsonUtil.jsonToIntegerMap(info.getDayOlDetail());
			for (Integer time : olMap.keySet()) {
				firstLoginTime = Math.min(firstLoginTime, time);
				lastLoginTime = Math.max(lastLoginTime, time);
			}

			// 新增当天设置首登时间
			if (info.getCreateTime() > 0 && 0 == info.getFirstLoginDate()) {
				put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_firstLoginTime,
						Bytes.toBytes(firstLoginTime));
			}

			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_lastLoginTime,
					Bytes.toBytes(lastLoginTime));

			System.out.println("roleOlTrack:" + info.getOlTrack());
			System.out.println("firstLogInDate:" + firstLogInDate);
			System.out.println("ts:" + ts);
			String newOLTrack = TrackUtil.addTargetDate2Track(info.getOlTrack(), firstLogInDate, ts);

			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_olTrack, Bytes.toBytes(newOLTrack));
		}

		if (info.getDayPayTimes() > 0) {
			int firstPayDate = info.getFirstPayDate();
			if (0 == info.getFirstPayDate()) {
				put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_firstPayDate, Bytes.toBytes(ts));
				firstPayDate = ts;
			}
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_lastPayDate, Bytes.toBytes(ts));
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_totalPayAmount,
					Bytes.toBytes(info.getTotalPayAmount() + info.getDayPayAmount()));
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_totalPayTimes,
					Bytes.toBytes(info.getTotalPayTimes() + info.getDayPayTimes()));

			int firstPayTime = Integer.MAX_VALUE;
			int lastPayTime = 0;
			// Map<Integer, Float> payMap = DCJsonUtil.getMapFromJson(info.getDayPayDetail(), tokenType, new Float(0));
			Map<Integer, Float> payMap = DCJsonUtil.jsonToIntFloatMap(info.getDayPayDetail());
			for (Integer time : payMap.keySet()) {
				firstPayTime = Math.min(firstPayTime, time);
				lastPayTime = Math.max(lastPayTime, time);
			}
			float firstPayAmount = payMap.get(firstPayTime);
			float lastPayAmount = payMap.get(lastPayTime);

			// 首付当天设置第一次付费时间
			if (0 == info.getFirstPayTime()) {
				put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_firstPayTime,
						Bytes.toBytes(firstPayTime));
				put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_firstPayAmount,
						Bytes.toBytes(firstPayAmount));
			}

			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_lastPayTime, Bytes.toBytes(lastPayTime));
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_lastPayAmount,
					Bytes.toBytes(lastPayAmount));

			String newPayTrack = TrackUtil.addTargetDate2Track(info.getPayTrack(), firstPayDate, ts);
			put.addColumn(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_payTrack, Bytes.toBytes(newPayTrack));
		}
	}

	/**
	 * <pre>
	 * Hbase查询结果组装成类
	 * @param info
	 * @param result
	 * @author Hayden<br>
	 * @date 2015年11月6日 下午5:04:20
	 * <br>
	 */
	private static void wrapResult2DynamicInfo(RoleHistoryInfo info, Result result) {

		// 区服
		byte[] gameServer = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_gameServer);
		if (null != gameServer) {
			info.setGameServer(Bytes.toString(gameServer));
		}

		// 原区服
		byte[] fromServer = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_fromServer);
		if (null != fromServer) {
			info.setFromServer(Bytes.toString(fromServer));
		}

		// accountId
		byte[] accountId = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_accountId);
		if (null != accountId) {
			info.setAccountId(Bytes.toString(accountId));
		}

		// accountType
		byte[] accountType = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_accountType);
		if (null != accountType) {
			info.setAccountType(Bytes.toString(accountType));
		}

		// accountType
		byte[] gender = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_gender);
		if (null != gender) {
			info.setGender(Bytes.toString(gender));
		}
		// accountType
		byte[] age = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_age);
		if (null != age) {
			info.setAge(Bytes.toString(age));
		}

		// // 角色ID
		byte[] roleId = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_roleId);
		if (null != roleId) {
			info.setUid(Bytes.toString(roleId));
		}
		// 角色种族
		byte[] roleRace = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_roleRace);
		if (null != roleRace) {
			info.setRoleRace(Bytes.toString(roleRace));
		}
		// 角色职业
		byte[] roleClass = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_roleClass);
		if (null != roleClass) {
			info.setRoleClass(Bytes.toString(roleClass));
		}
		// 角色等级
		byte[] roleLevel = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_roleLevel);
		if (null != roleLevel) {
			info.setRoleLevel(Bytes.toString(roleLevel));
		}
		// createTime
		byte[] createTime = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_createTime);
		if (null != createTime) {
			info.setCreateTime(Bytes.toInt(createTime));
		}
		// enableTime
		byte[] enableTime = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_enableTime);
		if (null != enableTime) {
			info.setEnableTime(Bytes.toInt(enableTime));
		}
		// deleteTime
		byte[] deleteTime = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_deleteTime);
		if (null != deleteTime) {
			info.setDeleteTime(Bytes.toInt(deleteTime));
		}

		// firstLoginDate
		byte[] firstLoginDate = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_firstLoginDate);
		if (null != firstLoginDate) {
			info.setFirstLoginDate(Bytes.toInt(firstLoginDate));
		}
		// firstLoginTime
		byte[] firstLoginTime = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_firstLoginTime);
		if (null != firstLoginTime) {
			info.setFirstLoginTime(Bytes.toInt(firstLoginTime));
		}
		// lastLoginDate
		byte[] lastLoginDate = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_lastLoginDate);
		if (null != lastLoginDate) {
			info.setLastLoginDate(Bytes.toInt(lastLoginDate));
		}
		// lastLoginTime
		byte[] lastLoginTime = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_lastLoginTime);
		if (null != lastLoginTime) {
			info.setLastLoginTime(Bytes.toInt(lastLoginTime));
		}
		// totalOnlineDays
		byte[] totalOnlineDays = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_totalOnlineDays);
		if (null != totalOnlineDays) {
			info.setTotalOnlineDays(Bytes.toInt(totalOnlineDays));
		}
		// totalLoginTimes
		byte[] totalLoginTimes = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_totalLoginTimes);
		if (null != totalLoginTimes) {
			info.setTotalLoginTimes(Bytes.toInt(totalLoginTimes));
		}
		// totalOnlineDays
		byte[] totalOnlineTime = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_totalOnlineTime);
		if (null != totalOnlineTime) {
			info.setTotalOnlineTime(Bytes.toInt(totalOnlineTime));
		}

		// totalOnlineDays
		byte[] firstPayDate = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_firstPayDate);
		if (null != firstPayDate) {
			info.setFirstPayDate(Bytes.toInt(firstPayDate));
		}
		// totalOnlineDays
		byte[] firstPayTime = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_firstPayTime);
		if (null != firstPayTime) {
			info.setFirstPayTime(Bytes.toInt(firstPayTime));
		}
		// totalOnlineDays
		byte[] firstPayAmount = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_firstPayAmount);
		if (null != firstPayAmount) {
			info.setFirstPayAmount(Bytes.toFloat(firstPayAmount));
		}
		// totalOnlineDays
		byte[] lastPayDate = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_lastPayDate);
		if (null != lastPayDate) {
			info.setLastPayDate(Bytes.toInt(lastPayDate));
		}
		// totalOnlineDays
		byte[] lastPayTime = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_lastPayTime);
		if (null != lastPayTime) {
			info.setLastPayTime(Bytes.toInt(lastPayTime));
		}
		// lastPayAmount
		byte[] lastPayAmount = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_lastPayAmount);
		if (null != lastPayAmount) {
			info.setLastPayAmount(Bytes.toFloat(lastPayAmount));
		}
		// totalOnlineDays
		byte[] totalPayAmount = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_totalPayAmount);
		if (null != totalPayAmount) {
			info.setTotalPayAmount(Bytes.toFloat(totalPayAmount));
		}

		// totalOnlineDays
		byte[] totalPayTimes = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_totalPayTimes);
		if (null != totalPayTimes) {
			info.setTotalPayTimes(Bytes.toInt(totalPayTimes));
		}
		// totalOnlineDays
		byte[] olTrack = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_olTrack);
		if (null != olTrack) {
			info.setOlTrack(Bytes.toString(olTrack));
		}
		// totalOnlineDays
		byte[] payTrack = result.getValue(RoleHistHBaseInfo.cf_dynamic, RoleHistHBaseInfo.d_q_payTrack);
		if (null != payTrack) {
			info.setPayTrack(Bytes.toString(payTrack));
		}

	}

	/**
	 * <pre>
	 * Hbase查询静态结果组装成类
	 * @param info
	 * @param result
	 * @author Hayden<br>
	 * @date 2015年11月6日 下午5:04:20
	 * <br>
	 */
	private static void wrapResult2StaticInfo(RoleHistoryInfo info, Result result) {

		byte[] timeStamp = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_time_stamp);
		if (null != timeStamp) {
			info.setTimestamp(Bytes.toInt(timeStamp));
		}

		byte[] appId = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_appId);
		if (null != appId) {
			info.setAppId(Bytes.toString(appId));
		}

		byte[] appVer = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_appVer);
		if (null != appVer) {
			info.setAppVer(Bytes.toString(appVer));
		}

		byte[] platform = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_plat);
		if (null != platform) {
			info.setPlatform(Bytes.toString(platform));
		}

		byte[] channel = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_ch);
		if (null != channel) {
			info.setChannel(Bytes.toString(channel));
		}

		byte[] country = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_cnty);
		if (null != country) {
			info.setCountry(Bytes.toString(country));
		}

		byte[] province = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_prov);
		if (null != province) {
			info.setProvince(Bytes.toString(province));
		}

		byte[] city = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_city);
		if (null != city) {
			info.setCity(Bytes.toString(city));
		}

		byte[] uid = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_uid);
		if (null != uid) {
			info.setUid(Bytes.toString(uid));
		}

		byte[] mac = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_mac);
		if (null != mac) {
			info.setMac(Bytes.toString(mac));
		}

		byte[] imei1 = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_imei1);
		if (null != imei1) {
			info.setImei1(Bytes.toString(imei1));
		}

		byte[] imsi1 = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_imsi1);
		if (null != imsi1) {
			info.setImsi1(Bytes.toString(imsi1));
		}

		byte[] imei2 = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_imei2);
		if (null != imei2) {
			info.setImei2(Bytes.toString(imei2));
		}

		byte[] imsi2 = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_imsi2);
		if (null != imsi2) {
			info.setImsi2(Bytes.toString(imsi2));
		}

		byte[] idfa = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_idfa);
		if (null != idfa) {
			info.setIdfa(Bytes.toString(idfa));
		}

		byte[] idfv = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_idfv);
		if (null != idfv) {
			info.setIdfv(Bytes.toString(idfv));
		}

		byte[] model = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_model);
		if (null != model) {
			info.setModel(Bytes.toString(model));
		}

		byte[] brand = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_brand);
		if (null != brand) {
			info.setBrand(Bytes.toString(brand));
		}

		byte[] manu = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_manu);
		if (null != manu) {
			info.setManu(Bytes.toString(manu));
		}

		byte[] screen = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_screen);
		if (null != screen) {
			info.setScreen(Bytes.toString(screen));
		}

		byte[] cpu = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_cpu);
		if (null != cpu) {
			info.setCpu(Bytes.toString(cpu));
		}

		byte[] root = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_root);
		if (null != root) {
			info.setRoot(Bytes.toString(root));
		}

		byte[] language = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_lang);
		if (null != language) {
			info.setLang(Bytes.toString(language));
		}

		byte[] oper = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_oper);
		if (null != oper) {
			info.setOper(Bytes.toString(oper));
		}

		byte[] operISO = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_operISO);
		if (null != operISO) {
			info.setOperISO(Bytes.toString(operISO));
		}

		byte[] os = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_os);
		if (null != os) {
			info.setOs(Bytes.toString(os));
		}

		byte[] timezone = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_tz);
		if (null != timezone) {
			info.setTimeZone(Bytes.toString(timezone));
		}

		byte[] ip = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_ip);
		if (null != ip) {
			info.setIp(Bytes.toString(ip));
		}

		byte[] net = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_net);
		if (null != net) {
			info.setNet(Bytes.toString(net));
		}

		byte[] longitude = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_lon);
		if (null != longitude) {
			info.setLongitude(Bytes.toString(longitude));
		}

		byte[] latitude = result.getValue(RoleHistHBaseInfo.cf_static, RoleHistHBaseInfo.s_q_lat);
		if (null != latitude) {
			info.setLatitude(Bytes.toString(latitude));
		}
	}
}
