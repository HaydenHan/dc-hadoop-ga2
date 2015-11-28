package com.dataeye.hadoop.mapreduce.v2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.dataeye.hadoop.common.dewritable.DEDynamicKV;
import com.dataeye.hadoop.domain.common.DEEventIds;
import com.dataeye.hadoop.domain.common.DENullWritable;
import com.dataeye.hadoop.domain.common.MRConstants;
import com.dataeye.hadoop.domain.common.MRConstants.PathConstant;
import com.dataeye.hadoop.domain.common.RawLog;
import com.dataeye.hadoop.domain.common.RawLog.Event;
import com.dataeye.hadoop.domain.common.RawLog.Header;
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
import com.dataeye.hadoop.util.DCDateUtil;
import com.dataeye.hadoop.util.DCJsonUtil;
import com.dataeye.hadoop.util.HBaseUtil;

public class RawLogProcessMapper extends Mapper<LongWritable, Text, DEDynamicKV, DEDynamicKV> {

	private DEDynamicKV dynamicKey = new DEDynamicKV();
	private DEDynamicKV dynamicVal = new DEDynamicKV();

	private boolean isAccountRollLog = false;
	private boolean isRoleRollLog = false;
	private boolean isNeedLastHourRolling = true;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		isNeedLastHourRolling = DCDateUtil.isNeedLastHourRolling(context.getConfiguration());
		HBaseUtil.init(context.getConfiguration());
		String fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
		if (fileSuffix.startsWith(PathConstant.BUSINESS_ACCOUNT_ROLLING)) {
			isAccountRollLog = true;
		} else if (fileSuffix.startsWith(PathConstant.BUSINESS_ROLE_ROLLING)) {
			isRoleRollLog = true;
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		if (isAccountRollLog) {
			if (!isNeedLastHourRolling) {
				return;
			}
			AccountRollInfo accountRoll = new AccountRollInfo(value.toString());
			MKAccountId accountId = new MKAccountId(accountRoll.getAppId(), accountRoll.getPlatform(),
					accountRoll.getGameServer(), accountRoll.getAccountId());
			dynamicKey.setDeWritable(accountId);
			dynamicVal.setDeWritable(accountRoll);
			context.write(dynamicKey, dynamicVal);
		} else if (isRoleRollLog) {
			if (!isNeedLastHourRolling) {
				return;
			}
			RoleRollInfo roleRoll = new RoleRollInfo(value.toString());
			MKRoleId roleId = new MKRoleId(roleRoll.getAppId(), roleRoll.getPlatform(), roleRoll.getGameServer(),
					roleRoll.getAccountId(), roleRoll.getRoleId());
			dynamicKey.setDeWritable(roleId);
			dynamicVal.setDeWritable(roleRoll);
			context.write(dynamicKey, dynamicVal);
		} else {
			RawLog input = null;
			try {
				input = DCJsonUtil.getGson().fromJson(value.toString(), RawLog.class);
			} catch (Exception e) {
				System.out.println(e);
				System.out.println(value.toString());
			}
			Header header = input.getHeader();
			if (!logFilter(header)) {
				System.out.println(value.toString());
				return;
			}
			Event[] events = input.getEvent();
			if (events == null || events.length <= 0) {
				System.out.println(value.toString());
				return;
			}
			for (Event event : events) {
				switch (event.getEventId()) {
				case DEEventIds.DESelf_Device_Active:
					handleDeviceActive(header, event, context);
					break;
				case DEEventIds.DESelf_Account_Create:
					handleAccountCreate(header, event, context);
					break;
				case DEEventIds.DESelf_Online:
					handleAccountOnline(header, event, context);
					// 角色在线信息
					handleRoleOnline(header, event, context);
					break;
				case DEEventIds.DESelf_Payment:
					handleAccountPay(header, event, context);
					// 角色付费信息
					handleRolePay(header, event, context);
					break;
				case DEEventIds.DESelf_Role_Create:
					// 角色创建
					handleRoleCreate(header, event, context);
					break;
				case DEEventIds.DESelf_Role_Delete:
					// 角色删除
					handleRoleDelete(header, event, context);
					break;
				case DEEventIds.DESelf_Role_Enable:
					// 角色激活
					handleRoleEnable(header, event, context);
					break;
				case DEEventIds.DESelf_Debug:
					;
					break;
				case DEEventIds.DESelf_Oss:
					;
					break;
				case DEEventIds.DESelf_Sdk_Init:
					;
					break;
				case DEEventIds.DESelf_Cell_Info:
					;
					break;
				case DEEventIds.DESelf_App_List:
					;
					break;
				case DEEventIds.DESelf_ErrorReport:
					;
					break;
				case DEEventIds.DESelf_UserDefined_ErrorReport:
					;
					break;
				case DEEventIds.DESelf_Player:
					;
					break;
				case DEEventIds.DESelf_Coin_Get:
					;
					break;
				case DEEventIds.DESelf_Coin_Use:
					;
					break;
				case DEEventIds.DESelf_Coin_Num:
					;
					break;
				case DEEventIds.DESelf_Item_Get:
					;
					break;
				case DEEventIds.DESelf_Item_Use:
					;
					break;
				case DEEventIds.DESelf_Levels_End:
					;
					break;
				case DEEventIds.DESelf_Task_End:
					;
					break;
				case DEEventIds.DESelf_Account_Login:
					;
					break;
				case DEEventIds.DESelf_Role_LevelUp:
					;
					break;
				default:
					// 自定义事件
					handleEventSelf(header, event, context);
					break;
				}
			}
		}
	}

	/**
	 * 统计设备激活及注册转换，同时把新增的设备信息记录到缓存中
	 * 
	 * @param header
	 * @param event
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void handleDeviceActive(Header header, Event event, Context context) throws IOException,
			InterruptedException {
		MKUID uid = new MKUID(header);
		MVUIDActive devieActive = new MVUIDActive(header, event);
		dynamicKey.setDeWritable(uid);
		dynamicVal.setDeWritable(devieActive);
		context.write(dynamicKey, dynamicVal);
	}

	private void handleAccountCreate(Header header, Event event, Context context) throws IOException,
			InterruptedException {
		MKAccountId accId = new MKAccountId(header, event);
		MVAccountCreate accountCreate = new MVAccountCreate(header, event);
		dynamicKey.setDeWritable(accId);
		dynamicVal.setDeWritable(accountCreate);
		context.write(dynamicKey, dynamicVal);
		// 输出全部区服
		accId.setGameServer(MRConstants.ALL_GS);
		accountCreate.setGameServer(MRConstants.ALL_GS);
		dynamicKey.setDeWritable(accId);
		dynamicVal.setDeWritable(accountCreate);
		context.write(dynamicKey, dynamicVal);
	}

	private void handleAccountOnline(Header header, Event event, Context context) throws IOException,
			InterruptedException {
		MKAccountId accId = new MKAccountId(header, event);
		MVAccountOnline accountOnline = new MVAccountOnline(header, event);
		dynamicKey.setDeWritable(accId);
		dynamicVal.setDeWritable(accountOnline);
		context.write(dynamicKey, dynamicVal);
		// 输出全部区服
		accId.setGameServer(MRConstants.ALL_GS);
		accountOnline.setGameServer(MRConstants.ALL_GS);
		dynamicKey.setDeWritable(accId);
		dynamicVal.setDeWritable(accountOnline);
		context.write(dynamicKey, dynamicVal);

	}

	private void handleAccountPay(Header header, Event event, Context context) throws IOException, InterruptedException {
		MKAccountId accId = new MKAccountId(header, event);
		MVAccountPay accountPay = new MVAccountPay(header, event);
		dynamicKey.setDeWritable(accId);
		dynamicVal.setDeWritable(accountPay);
		context.write(dynamicKey, dynamicVal);
		// 输出全部区服
		accId.setGameServer(MRConstants.ALL_GS);
		accountPay.setGameServer(MRConstants.ALL_GS);
		dynamicKey.setDeWritable(accId);
		dynamicVal.setDeWritable(accountPay);
		context.write(dynamicKey, dynamicVal);
	}

	/**
	 * <pre>
	 * 创建角色
	 * @param header
	 * @param event
	 * @param context
	 * @author Hayden<br>
	 * @throws InterruptedException 
	 * @throws IOException 
	 * @date 2015年11月5日 下午3:33:24
	 * <br>
	 */
	private void handleRoleCreate(Header header, Event event, Context context) throws IOException, InterruptedException {
		if (event.existRoleId()) {
			MKRoleId roleId = new MKRoleId(header, event);
			MVRoleCreate roleCreate = new MVRoleCreate(header, event);
			dynamicKey.setDeWritable(roleId);
			dynamicVal.setDeWritable(roleCreate);
			context.write(dynamicKey, dynamicVal);

			// 多输出一份用于判断帐号注册当天是否创建角色
			MKAccountId accountId = new MKAccountId(header, event);
			dynamicKey.setDeWritable(accountId);
			dynamicVal.setDeWritable(roleCreate);
			context.write(dynamicKey, dynamicVal);

			// 角色创建输出全部区服
			roleId.setGameServer(MRConstants.ALL_GS);
			roleCreate.setGameServer(MRConstants.ALL_GS);
			dynamicKey.setDeWritable(roleId);
			dynamicVal.setDeWritable(roleCreate);
			context.write(dynamicKey, dynamicVal);

			// 多输出一份用于判断帐号注册当天是否创建角色，全部区服
			accountId.setGameServer(MRConstants.ALL_GS);
			dynamicKey.setDeWritable(accountId);
			dynamicVal.setDeWritable(roleCreate);
			context.write(dynamicKey, dynamicVal);
		}
	}

	/**
	 * <pre>
	 * 角色激活
	 * @param header
	 * @param event
	 * @param context
	 * @author Hayden<br>
	 * @throws InterruptedException 
	 * @throws IOException 
	 * @date 2015年11月5日 下午3:45:55
	 * <br>
	 */
	private void handleRoleEnable(Header header, Event event, Context context) throws IOException, InterruptedException {
		if (event.existRoleId()) {
			MKRoleId roleId = new MKRoleId(header, event);
			MVRoleEnable roleEnable = new MVRoleEnable(header, event);
			dynamicKey.setDeWritable(roleId);
			dynamicVal.setDeWritable(roleEnable);
			context.write(dynamicKey, dynamicVal);
			// 输出全部区服
			roleId.setGameServer(MRConstants.ALL_GS);
			roleEnable.setGameServer(MRConstants.ALL_GS);
			dynamicKey.setDeWritable(roleId);
			dynamicVal.setDeWritable(roleEnable);
			context.write(dynamicKey, dynamicVal);
		}
	}

	/**
	 * <pre>
	 * 角色在线
	 * @param header
	 * @param event
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 * @author Hayden<br>
	 * @date 2015年11月5日 下午3:33:39
	 * <br>
	 */
	private void handleRoleOnline(Header header, Event event, Context context) throws IOException, InterruptedException {
		if (event.existRoleId()) {
			MKRoleId roleId = new MKRoleId(header, event);
			MVRoleOnline roleOnline = new MVRoleOnline(header, event);
			dynamicKey.setDeWritable(roleId);
			dynamicVal.setDeWritable(roleOnline);
			context.write(dynamicKey, dynamicVal);
			// 输出全部区服
			roleId.setGameServer(MRConstants.ALL_GS);
			roleOnline.setGameServer(MRConstants.ALL_GS);
			dynamicKey.setDeWritable(roleId);
			dynamicVal.setDeWritable(roleOnline);
			context.write(dynamicKey, dynamicVal);
		}
	}

	/**
	 * <pre>
	 * 角色付费
	 * @param header
	 * @param event
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 * @author Hayden<br>
	 * @date 2015年11月5日 下午3:33:52
	 * <br>
	 */
	private void handleRolePay(Header header, Event event, Context context) throws IOException, InterruptedException {
		if (event.existRoleId()) {
			MKRoleId roleId = new MKRoleId(header, event);
			MVRolePay rolePay = new MVRolePay(header, event);
			dynamicKey.setDeWritable(roleId);
			dynamicVal.setDeWritable(rolePay);
			context.write(dynamicKey, dynamicVal);
			// 输出全部区服
			roleId.setGameServer(MRConstants.ALL_GS);
			rolePay.setGameServer(MRConstants.ALL_GS);
			dynamicKey.setDeWritable(roleId);
			dynamicVal.setDeWritable(rolePay);
			context.write(dynamicKey, dynamicVal);
		}
	}

	/**
	 * <pre>
	 * 角色删除
	 * @param header
	 * @param event
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 * @author Hayden<br>
	 * @date 2015年11月5日 下午3:44:06
	 * <br>
	 */
	private void handleRoleDelete(Header header, Event event, Context context) throws IOException, InterruptedException {
		if (event.existRoleId()) {
			MKRoleId roleId = new MKRoleId(header, event);
			MVRoleDelete roleDelete = new MVRoleDelete(header, event);
			dynamicKey.setDeWritable(roleId);
			dynamicVal.setDeWritable(roleDelete);
			context.write(dynamicKey, dynamicVal);
			// 输出全部区服
			roleId.setGameServer(MRConstants.ALL_GS);
			roleDelete.setGameServer(MRConstants.ALL_GS);
			dynamicKey.setDeWritable(roleId);
			dynamicVal.setDeWritable(roleDelete);
			context.write(dynamicKey, dynamicVal);
		}
	}

	/**
	 * <pre>
	 * 自定义事件
	 * @param header
	 * @param event
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 * @author Hayden<br>
	 * @date 2015年11月17日 上午11:09:23
	 * <br>
	 */
	private void handleEventSelf(Header header, Event event, Context context) throws IOException, InterruptedException {
		MKEventSelf eventSelf = new MKEventSelf(header, event);
		dynamicKey.setDeWritable(eventSelf);
		dynamicVal.setDeWritable(DENullWritable.get());
		context.write(dynamicKey, dynamicVal);
		// 输出全部区服
		eventSelf.setGameserver(MRConstants.ALL_GS);
		dynamicKey.setDeWritable(eventSelf);
		context.write(dynamicKey, dynamicVal);
	}

	/**
	 * <pre>
	 * 初步判断日志是否复合要求，符合要求进行计算
	 * @param header
	 * @return
	 * @author Hayden<br>
	 * @date 2015年11月20日 下午4:42:06
	 * <br>
	 */
	private boolean logFilter(Header header) {
		if (header == null) {
			return false;
		}
		if (MRConstants.STR_PLACE_HOLDER.equals(header.getAppId())) {
			return false;
		}
		if (MRConstants.STR_PLACE_HOLDER.equals(header.getPlat())) {
			return false;
		}
		return true;
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	}
}
