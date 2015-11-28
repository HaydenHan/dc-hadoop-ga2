package com.dataeye.hadoop.mapreduce.v2;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.dataeye.hadoop.common.dewritable.DEDynamicKV;
import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.DENullWritable;
import com.dataeye.hadoop.domain.common.MRConstants;
import com.dataeye.hadoop.domain.common.MRConstants.PathConstant;
import com.dataeye.hadoop.domain.hbase.RoleHistoryInfo;
import com.dataeye.hadoop.domain.kv.AccountRollInfo;
import com.dataeye.hadoop.domain.kv.MKAccountId;
import com.dataeye.hadoop.domain.kv.MKEventSelf;
import com.dataeye.hadoop.domain.kv.MKUID;
import com.dataeye.hadoop.domain.kv.MVAccountCreate;
import com.dataeye.hadoop.domain.kv.MVAccountOnline;
import com.dataeye.hadoop.domain.kv.MVAccountPay;
import com.dataeye.hadoop.domain.kv.MVUIDActive;
import com.dataeye.hadoop.domain.kv.role.MKRoleId;
import com.dataeye.hadoop.domain.kv.role.MVRoleCreate;
import com.dataeye.hadoop.domain.kv.role.MVRoleDelete;
import com.dataeye.hadoop.domain.kv.role.MVRoleEnable;
import com.dataeye.hadoop.domain.kv.role.MVRoleOnline;
import com.dataeye.hadoop.domain.kv.role.MVRolePay;
import com.dataeye.hadoop.domain.kv.role.RoleRollInfo;
import com.dataeye.hadoop.util.AccountCacheUtil;
import com.dataeye.hadoop.util.DCDateUtil;
import com.dataeye.hadoop.util.DCJsonUtil;
import com.dataeye.hadoop.util.DeviceCacheUtil;
import com.dataeye.hadoop.util.HBaseUtil;
import com.dataeye.hadoop.util.RoleCacheUtil;
import com.dataeye.hadoop.util.StringUtil;

public class RawLogProcessReducer extends Reducer<DEDynamicKV, DEDynamicKV, DEDynamicKV, DEDynamicKV> {

	private DEDynamicKV dynamicKey = new DEDynamicKV();
	private DEDynamicKV dynamicVal = new DEDynamicKV();
	private MultipleOutputs<DEDynamicKV, DEDynamicKV> mos;
	private int dataDate = 0;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<DEDynamicKV, DEDynamicKV>(context);
		dataDate = DCDateUtil.getDataDate(context.getConfiguration());
		HBaseUtil.init(context.getConfiguration());
	}

	@Override
	protected void reduce(DEDynamicKV key, Iterable<DEDynamicKV> vals, Context context) throws IOException,
			InterruptedException {

		if (key.getDeWritable() instanceof MKUID) {
			// 设备激活处理
			handleDeviceInfo(vals, context);
		} else if (key.getDeWritable() instanceof MKAccountId) {
			// 玩家新增在线付费信息处理
			handleAccountInfo(vals, context);
		} else if (key.getDeWritable() instanceof MKRoleId) {
			// 角色新增在线付费信息处理
			handleRoleInfo((MKRoleId) key.getDeWritable(), vals, context);
		} else if (key.getDeWritable() instanceof MKEventSelf) {
			handleEventSelf((MKEventSelf) key.getDeWritable(), vals, context);
		}
	}

	/**
	 * 
	 * @param vals
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void handleDeviceInfo(Iterable<DEDynamicKV> vals, Context context) throws IOException, InterruptedException {
		MVUIDActive uidActive = null;
		for (DEDynamicKV val : vals) {
			MVUIDActive active = (MVUIDActive) val.getDeWritable();
			if (null == uidActive) {
				uidActive = new MVUIDActive(active);
				// 第一次碰到是激活则检查是否已激活过
				boolean isAlreadyExist = DeviceCacheUtil.isUidExist(uidActive.getAppId(), uidActive.getPlatform(),
						uidActive.getUid());
				uidActive.setIsRealActive(isAlreadyExist ? MRConstants.INT_FALSE : MRConstants.INT_TRUE);
			} else if (active.getTimestamp() < uidActive.getTimestamp()) {
				// 不能覆盖已经检查过的缓存状态
				int activeStatus = uidActive.getIsRealActive();
				uidActive = new MVUIDActive(active);
				uidActive.setIsRealActive(activeStatus);
			}
		}
		uidActive.setTimestamp(DCDateUtil.getStatDate(uidActive));
		dynamicKey.setDeWritable(uidActive);
		dynamicVal.setDeWritable(DENullWritable.get());
		// context.write(dynamicKey, dynamicVal);
		mos.write(dynamicKey, dynamicVal,
				StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_DEVICE_ACTIVE));
	}

	/**
	 * 
	 * @param vals
	 * @param context
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private void handleAccountInfo(Iterable<DEDynamicKV> vals, Context context) throws IOException,
			InterruptedException {
		AccountRollInfo accRollInfo = null;
		MVAccountCreate accountCreate = null;
		MVAccountOnline accountOnline = null;
		MVAccountPay accoutPay = null;
		MVRoleCreate roleCreate = null;
		Map<Integer, Integer> onlineMap = new HashMap<Integer, Integer>();
		Map<Integer, Float> payMap = new HashMap<Integer, Float>();
		Set<String> newRoleSet = new HashSet<String>();

		for (DEDynamicKV val : vals) {
			DEWritable instance = val.getDeWritable();
			if (instance instanceof AccountRollInfo) {
				accRollInfo = (AccountRollInfo) instance;
			} else if (instance instanceof MVAccountCreate) {
				MVAccountCreate tmpAccCreate = (MVAccountCreate) instance;
				// 取时间最小的记录
				if (null == accountCreate || tmpAccCreate.getTimestamp() < accountCreate.getTimestamp()) {
					accountCreate = new MVAccountCreate(tmpAccCreate);
				}
			} else if (instance instanceof MVAccountOnline) {
				MVAccountOnline tmpAccOnline = (MVAccountOnline) instance;
				Integer onlineTime = onlineMap.get(tmpAccOnline.getLoginTime());
				if (null == onlineTime) {
					onlineMap.put(tmpAccOnline.getLoginTime(), tmpAccOnline.getOnlineTime());
				} else {
					onlineMap.put(tmpAccOnline.getLoginTime(), Math.max(onlineTime, tmpAccOnline.getOnlineTime()));
				}
				// 取时间最小的记录
				if (null == accountOnline || tmpAccOnline.getTimestamp() < accountOnline.getTimestamp()) {
					accountOnline = new MVAccountOnline(tmpAccOnline);
				}
			} else if (instance instanceof MVAccountPay) {
				MVAccountPay tmpAccPay = (MVAccountPay) instance;
				Float payAmount = payMap.get(tmpAccPay.getPayTime());
				if (null == payAmount) {
					payMap.put(tmpAccPay.getPayTime(), tmpAccPay.getPayAmount());
				} else {
					// 理论上，一个 payTime 只有一个 payAmount
					payMap.put(tmpAccPay.getPayTime(), Math.max(payAmount, tmpAccPay.getPayAmount()));
				}

				// 取时间最小的记录
				if (null == accoutPay || tmpAccPay.getTimestamp() < accoutPay.getTimestamp()) {
					accoutPay = new MVAccountPay(tmpAccPay);
				}
			} else if (instance instanceof MVRoleCreate) {
				MVRoleCreate tmpRoleCreate = (MVRoleCreate) instance;
				newRoleSet.add(tmpRoleCreate.getRoleId());
				// 取时间最小的记录
				if (null == roleCreate || tmpRoleCreate.getTimestamp() < roleCreate.getTimestamp()) {
					roleCreate = new MVRoleCreate(tmpRoleCreate);
				}
			}
		}

		// 多输出一份 AccountCreate 用于注册转化统计
		if (null != accountCreate) {
			MVAccountCreate accountCreateTmp = new MVAccountCreate(accountCreate);
			accountCreateTmp.setTimestamp(DCDateUtil.getStatDate(accountCreate));
			dynamicKey.setDeWritable(accountCreateTmp);
			dynamicVal.setDeWritable(DENullWritable.get());
			// context.write(dynamicKey, dynamicVal);
			mos.write(dynamicKey, dynamicVal,
					StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ACCOUNT_CREATE));
		}

		if (null == accRollInfo) {
			// 未查询过缓存查，用现有数据设置基本信息
			// basic info 设置优先级为：create-->online-->pay
			if (null != accountCreate) {
				accRollInfo = new AccountRollInfo(accountCreate);
				// 设置是否今天创建（不一定是新增，可能重复创建）
				accRollInfo.setDayCreateTime(accountCreate.getCreateTime());
			} else if (null != accountOnline) {
				accRollInfo = new AccountRollInfo(accountOnline);
			} else if (null != accoutPay) {
				accRollInfo = new AccountRollInfo(accoutPay);
			} else if (null != roleCreate) {
				accRollInfo = new AccountRollInfo(roleCreate);
			}

			// 从缓存查询历史信息并设置到 accRollInfo 中
			AccountCacheUtil.getAndSetHistoryInfo(accRollInfo, dataDate);

			// 查询滚服信息并设置到 accRollInfo 中
			AccountCacheUtil.getAndSetFromGS(accRollInfo, dataDate);
		}

		// accRollInfo.setDayOlDetail(DCJsonUtil.getGson().toJson(onlineMap));
		accRollInfo.setDayOlDetail(StringUtil.mergeOlDetail(onlineMap, accRollInfo.getDayOlDetail()));
		// accRollInfo.setDayPayDetail(DCJsonUtil.getGson().toJson(payMap));
		accRollInfo.setDayPayDetail(StringUtil.mergePayDetail(payMap, accRollInfo.getDayPayDetail()));

		int dayLoginTimes = onlineMap.size();
		int dayOnlineTime = 0;
		for (Integer olt : onlineMap.values()) {
			dayOnlineTime += olt;
		}

		int dayPayTimes = payMap.size();
		float dayPayAmount = 0;
		for (Float pay : payMap.values()) {
			dayPayAmount += pay;
		}

		accRollInfo.setDayLoginTimes(dayLoginTimes);
		accRollInfo.setDayOnlineTime(dayOnlineTime);
		accRollInfo.setDayPayTimes(dayPayTimes);
		accRollInfo.setDayPayAmount(dayPayAmount);
		// set new create roleIds
		accRollInfo.setDayRoleIds(StringUtil.mergeRoleIds(newRoleSet, accRollInfo.getDayRoleIds()));
		// accRollInfo.setTimestamp(DCDateUtil.getStatDate(accRollInfo));
		dynamicKey.setDeWritable(accRollInfo);
		dynamicVal.setDeWritable(DENullWritable.get());
		// context.write(dynamicKey, dynamicVal);
		mos.write(dynamicKey, dynamicVal,
				StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ACCOUNT_ROLLING));
	}

	private void handleRoleInfo(MKRoleId roleId, Iterable<DEDynamicKV> vals, Context context) throws IOException,
			InterruptedException {
		RoleRollInfo roleRollInfo = null;
		MVRoleCreate roleCreate = null;
		MVRoleEnable roleEnable = null;
		MVRoleOnline roleOnline = null;
		MVRolePay rolePay = null;
		MVRoleDelete roleDelete = null;
		Map<Integer, Integer> onlineMap = new HashMap<Integer, Integer>();
		Map<Integer, Float> payMap = new HashMap<Integer, Float>();
		for (DEDynamicKV val : vals) {
			DEWritable instance = val.getDeWritable();
			if (instance instanceof RoleRollInfo) {
				// 滚存只有一条，原样输出
				roleRollInfo = (RoleRollInfo) instance;
			} else if (instance instanceof MVRoleCreate) {
				// 创角一般只有一条记录，如果有多条则取时间最早的一条（重复创角）
				MVRoleCreate tmpRoleCreate = (MVRoleCreate) instance;
				// boolean isAlreadyExist = false;
				boolean isAlreadyExist = RoleCacheUtil.isRoleExist(tmpRoleCreate.getAppId(),
						tmpRoleCreate.getPlatform(), tmpRoleCreate.getGameServer(), tmpRoleCreate.getAccountId(),
						tmpRoleCreate.getRoleId());
				if (null == roleCreate || tmpRoleCreate.getCreateTime() < roleCreate.getCreateTime()) {
					roleCreate = new MVRoleCreate(tmpRoleCreate);
					roleCreate.setIsNewCreate(isAlreadyExist ? MRConstants.INT_FALSE : MRConstants.INT_TRUE);
				}
			} else if (instance instanceof MVRoleEnable) {
				// 角色激活一般只有一条记录，如果有多条则取时间最早的一条（重复激活）
				MVRoleEnable tmpRoleEnable = (MVRoleEnable) instance;
				if (null == roleEnable || tmpRoleEnable.getEnableTime() < roleEnable.getEnableTime()) {
					roleEnable = new MVRoleEnable(tmpRoleEnable);
				}
			} else if (instance instanceof MVRoleOnline) {
				// 在线日志会有多条，登录时间一样时取最长在线时长作为在线时长
				MVRoleOnline tmpRoleOnline = (MVRoleOnline) instance;
				Integer onlineTime = onlineMap.get(tmpRoleOnline.getLoginTime());
				if (null == onlineTime) {
					onlineMap.put(tmpRoleOnline.getLoginTime(), tmpRoleOnline.getOnlineTime());
				} else {
					onlineMap.put(tmpRoleOnline.getLoginTime(), Math.max(onlineTime, tmpRoleOnline.getOnlineTime()));
				}
				// 登录时间最早的一条记录
				if (null == roleOnline || tmpRoleOnline.getLoginTime() < roleOnline.getLoginTime()) {
					roleOnline = new MVRoleOnline(tmpRoleOnline);
				}
			} else if (instance instanceof MVRolePay) {
				MVRolePay tmpRolePay = (MVRolePay) instance;
				Float payAmount = payMap.get(tmpRolePay.getPayTime());
				if (null == payAmount) {
					payMap.put(tmpRolePay.getPayTime(), tmpRolePay.getPayAmount());
				} else {
					// 理论上，一个 payTime 只有一个 payAmount
					payMap.put(tmpRolePay.getPayTime(), Math.max(payAmount, tmpRolePay.getPayAmount()));
				}
				// 取付费时间最早的记录
				if (null == rolePay || tmpRolePay.getTimestamp() < rolePay.getTimestamp()) {
					rolePay = new MVRolePay(tmpRolePay);
				}
			} else if (instance instanceof MVRoleDelete) {
				// 角色删除一般只有一条记录，如果有多条则取时间最早的一条（重复删除）
				MVRoleDelete tmpRoleDelete = (MVRoleDelete) instance;
				if (null == roleDelete || tmpRoleDelete.getDeleteTime() < roleDelete.getDeleteTime()) {
					roleDelete = new MVRoleDelete(tmpRoleDelete);
				}
			}
		}

		if (null == roleRollInfo) {
			// 未查询过缓存查，用现有数据设置基本信息
			// basic info 设置优先级为：create-->enable-->online-->pay-->delete
			if (null != roleCreate) {
				roleRollInfo = new RoleRollInfo(roleCreate);
				roleRollInfo.setCreateTime(roleCreate.getCreateTime());
				roleRollInfo.setIsNewCreate(roleCreate.getIsNewCreate());
			} else if (null != roleEnable) {
				roleRollInfo = new RoleRollInfo(roleEnable);
				roleRollInfo.setEnableTime(roleEnable.getEnableTime());
			} else if (null != roleOnline) {
				roleRollInfo = new RoleRollInfo(roleOnline);
			} else if (null != rolePay) {
				roleRollInfo = new RoleRollInfo(rolePay);
			} else if (null != roleDelete) {
				roleRollInfo = new RoleRollInfo(roleDelete);
				roleRollInfo.setDeleteTime(roleDelete.getDeleteTime());
			}

			// 从缓存查询历史信息
			RoleHistoryInfo histInfo = RoleCacheUtil.getRoleHistoryInfo(roleId.getAppId(), roleId.getPlatform(),
					roleId.getGameServer(), roleId.getAccountId(), roleId.getRoleId(), dataDate);
			roleRollInfo.setHistoryInfo(histInfo, dataDate);
		}

		// roleRollInfo.setDayOlDetail(DCJsonUtil.getGson().toJson(onlineMap));
		roleRollInfo.setDayOlDetail(StringUtil.mergeOlDetail(onlineMap, roleRollInfo.getDayOlDetail()));
		// roleRollInfo.setDayPayDetail(DCJsonUtil.getGson().toJson(payMap));
		roleRollInfo.setDayPayDetail(StringUtil.mergePayDetail(payMap, roleRollInfo.getDayPayDetail()));

		// 登录次数，在线时长
		int dayLoginTimes = onlineMap.size();
		int dayOnlineTime = 0;
		for (Integer olt : onlineMap.values()) {
			dayOnlineTime += olt;
		}
		// 付费次数，付费金额
		int dayPayTimes = payMap.size();
		float dayPayAmount = 0;
		for (Float pay : payMap.values()) {
			dayPayAmount += pay;
		}
		roleRollInfo.setDayLoginTimes(dayLoginTimes);
		roleRollInfo.setDayOnlineTime(dayOnlineTime);
		roleRollInfo.setDayPayTimes(dayPayTimes);
		roleRollInfo.setDayPayAmount(dayPayAmount);

		// 输出最终结果时才需根据时区设置 timestamp 为对应日期
		// roleRollInfo.setTimestamp(DCDateUtil.getStatDate(roleRollInfo));
		dynamicKey.setDeWritable(roleRollInfo);
		dynamicVal.setDeWritable(DENullWritable.get());
		// context.write(dynamicKey, dynamicVal);
		mos.write(dynamicKey, dynamicVal,
				StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ROLE_ROLLING));
	}
	
	private void handleEventSelf(MKEventSelf eventSelf, Iterable<DEDynamicKV> vals, Context context)
			throws IOException, InterruptedException {
		eventSelf.setTimestamp(DCDateUtil.date2yyyyMMddInt(eventSelf.getTimestamp() * 1000L));
		dynamicKey.setDeWritable(eventSelf);
		dynamicVal.setDeWritable(DENullWritable.get());
		mos.write(dynamicKey, dynamicVal,
				StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_EVENT_ATTR));
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		mos.close();
	}

	public static void main(String[] args) {
		System.out.println(DCJsonUtil.getGson().toJson(new HashMap<Integer, Integer>()));
	}

}
