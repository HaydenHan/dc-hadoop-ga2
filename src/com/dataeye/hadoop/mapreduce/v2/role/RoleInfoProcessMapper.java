package com.dataeye.hadoop.mapreduce.v2.role;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.dataeye.hadoop.common.dewritable.DEDynamicKV;
import com.dataeye.hadoop.domain.common.DENullWritable;
import com.dataeye.hadoop.domain.common.MRConstants;
import com.dataeye.hadoop.domain.common.MRConstants.PathConstant;
import com.dataeye.hadoop.domain.kv.role.RoleRollInfo;
import com.dataeye.hadoop.domain.result.role.RoleBackInfo;
import com.dataeye.hadoop.domain.result.role.RoleDayInfo;
import com.dataeye.hadoop.domain.result.role.RoleLTVInfo;
import com.dataeye.hadoop.domain.result.role.RoleRetainInfo;
import com.dataeye.hadoop.util.DCDateUtil;
import com.dataeye.hadoop.util.StringUtil;
import com.dataeye.hadoop.util.TrackUtil;

public class RoleInfoProcessMapper extends Mapper<LongWritable, Text, DEDynamicKV, DEDynamicKV> {

	private DEDynamicKV dynamicKey = new DEDynamicKV();
	private DEDynamicKV dynamicVal = new DEDynamicKV();
	private MultipleOutputs<DEDynamicKV, DEDynamicKV> mos;
	private int dataDate = 0;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<DEDynamicKV, DEDynamicKV>(context);
		dataDate = DCDateUtil.getDataDate(context.getConfiguration());
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		RoleRollInfo info = new RoleRollInfo(value.toString());
		// 角色新增活跃付费信息
		handleRoleDayInfo(info, context);
		// 角色留存
		handleRoleRetain(info, context);
		// 角色回流
		handleRoleBack(info, context);
		// 角色LTV
		handleRoleLTV(info, context);
	}

	/**
	 * <pre>
	 * 角色新增，活跃，付费信息。对应中间表2.1
	 * @param info
	 * @param context
	 * @author Hayden<br>
	 * @throws InterruptedException 
	 * @throws IOException 
	 * @date 2015年11月6日 上午10:37:51
	 * <br>
	 */
	private void handleRoleDayInfo(RoleRollInfo rollInfo, Context context) throws IOException, InterruptedException {
		RoleDayInfo dayInfo = new RoleDayInfo(rollInfo);
		// 是否新增
		dayInfo.setIsNewRole(rollInfo.getIsNewCreate());
		// 判断付费
		if (rollInfo.getDayPayAmount() > 0) {
			dayInfo.setIsPayRole(MRConstants.INT_TRUE);
		}
		if (rollInfo.getFirstLoginDate() <= 0 && rollInfo.getDayPayAmount() > 0) {
			// 新增首日付费
			dayInfo.setIsPayAtFirstDay(MRConstants.INT_TRUE);
		}
		// 判断曾经付费
		if (rollInfo.getTotalPayAmount() > 0 || rollInfo.getDayPayAmount() > 0) {
			dayInfo.setIsEverPayRole(MRConstants.INT_TRUE);
		}
		// 设置今日信息
		dayInfo.setDayLoginTimes(rollInfo.getDayLoginTimes());
		dayInfo.setDayOnlineTime(rollInfo.getDayOnlineTime());
		dayInfo.setDayPayTimes(rollInfo.getDayPayTimes());
		dayInfo.setDayPayAmount(rollInfo.getDayPayAmount());
		// 设置汇总信息
		if (rollInfo.getDayLoginTimes() > 0) {
			dayInfo.setTotalOnlineDays(1 + rollInfo.getTotalOnlineDays());
		} else {
			dayInfo.setTotalOnlineDays(rollInfo.getTotalOnlineDays());
		}
		dayInfo.setTotalOnlineTime(rollInfo.getTotalOnlineTime() + rollInfo.getDayOnlineTime());
		dayInfo.setTotalLoginTimes(rollInfo.getTotalLoginTimes() + rollInfo.getDayLoginTimes());
		dayInfo.setTotalPayAmount(rollInfo.getTotalPayAmount() + rollInfo.getDayPayAmount());
		dayInfo.setTotalPayTimes(rollInfo.getTotalPayTimes() + rollInfo.getDayPayTimes());
		dayInfo.setTimestamp(DCDateUtil.getStatDate(dayInfo));
		// 输出
		dynamicKey.setDeWritable(dayInfo);
		dynamicVal.setDeWritable(DENullWritable.get());
		// context.write(dynamicKey, dynamicVal);
		mos.write(dynamicKey, dynamicVal,
				StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ROLE_DAYINFO));
	}

	/**
	 * <pre>
	 * 角色留存
	 * @param rollInfo
	 * @param context
	 * @author Hayden<br>
	 * @throws InterruptedException 
	 * @throws IOException 
	 * @date 2015年11月6日 上午11:38:52
	 * <br>
	 */
	private void handleRoleRetain(RoleRollInfo rollInfo, Context context) throws IOException, InterruptedException {
		RoleRetainInfo retainInfo = new RoleRetainInfo(rollInfo);
		retainInfo.setTimestamp(DCDateUtil.getStatDate(retainInfo));
		for (int i = 0; i < MRConstants.RETAIN_MAX_DAYS; i++) {
			int targetDate = dataDate - i * MRConstants.DAY_IN_SECONDS;
			if (!TrackUtil.isTrackOnByDay(rollInfo.getOlTrack(), rollInfo.getFirstLoginDate(), targetDate)) {
				continue;
			}
			// 判断新增
			if (rollInfo.getFirstLoginDate() == targetDate) {
				retainInfo.setIsNewRole(MRConstants.INT_TRUE);
			}
			// 判断留存当天是否付费
			if (TrackUtil.isTrackOnByDay(rollInfo.getPayTrack(), rollInfo.getFirstPayDate(), targetDate)) {
				retainInfo.setIsPayRole(MRConstants.INT_TRUE);
			}
			// 判断曾经付费
			if (rollInfo.getTotalPayAmount() > 0 || rollInfo.getDayPayAmount() > 0) {
				retainInfo.setIsEverPayRole(MRConstants.INT_TRUE);
			}

			retainInfo.setActionTimes(MRConstants.RETAIN_ACTION_TIMES);
			retainInfo.setTargetDate(targetDate);
			retainInfo.setnDayRetain(i + 1);
			dynamicKey.setDeWritable(retainInfo);
			dynamicVal.setDeWritable(DENullWritable.get());
			// context.write(dynamicKey, dynamicVal);
			mos.write(dynamicKey, dynamicVal,
					StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ROLE_RETAIN));
		}
	}

	/**
	 * <pre>
	 * 角色回流
	 * @param rollInfo
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 * @author Hayden<br>
	 * @date 2015年11月6日 下午4:02:34
	 * <br>
	 */
	private void handleRoleBack(RoleRollInfo rollInfo, Context context) throws IOException, InterruptedException {
		if (rollInfo.getLastLoginDate() <= 0) {
			return;
		}
		RoleBackInfo backInfo = new RoleBackInfo(rollInfo);

		// 判断是否曾经付费
		if (rollInfo.getTotalPayAmount() > 0 || rollInfo.getDayPayAmount() > 0) {
			backInfo.setIsEverPay(MRConstants.INT_TRUE);
		}
		backInfo.setTimestamp(DCDateUtil.getStatDate(backInfo));
		dynamicKey.setDeWritable(backInfo);
		dynamicVal.setDeWritable(DENullWritable.get());
		int lostDays = (dataDate - rollInfo.getLastLoginDate()) / MRConstants.DAY_IN_SECONDS;
		// 需要计算流失的天数，3日，7日，14日回流
		Integer[] dayLostArr = new Integer[] { MRConstants.LOST_DAYS_3, MRConstants.LOST_DAYS_7,
				MRConstants.LOST_DAYS_14 };
		for (int i = 0, j = dayLostArr.length; i < j; i++) {
			int tarLostDay = dayLostArr[i];
			if (lostDays >= tarLostDay) {
				backInfo.setnDayBack(lostDays);
				// context.write(dynamicKey, dynamicVal);
				mos.write(dynamicKey, dynamicVal,
						StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ROLE_BACK));
			}
		}
	}

	/**
	 * <pre>
	 * 角色LTV
	 * @param rollInfo
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 * @author Hayden<br>
	 * @date 2015年11月6日 下午4:10:56
	 * <br>
	 */
	private void handleRoleLTV(RoleRollInfo rollInfo, Context context) throws IOException, InterruptedException {
		float paySumAmount = rollInfo.getTotalPayAmount() + rollInfo.getDayPayAmount();
		int nDay = 0;
		int firstLoginDate = rollInfo.getFirstLoginDate();
		if (0 == rollInfo.getFirstLoginDate()) {
			nDay = 1;
			firstLoginDate = dataDate + MRConstants.DAY_IN_SECONDS;
		} else {
			nDay = (dataDate - rollInfo.getFirstLoginDate()) / MRConstants.DAY_IN_SECONDS + 2;
		}
		if (paySumAmount <= 0 || nDay > MRConstants.LTV_MAX_DAYS) {
			return;
		}

		RoleLTVInfo LTVInfo = new RoleLTVInfo(rollInfo);
		LTVInfo.setFirstLoginDate(firstLoginDate);
		LTVInfo.setIsEverPay(MRConstants.INT_TRUE);
		LTVInfo.setnDayLife(nDay);
		LTVInfo.setnDayValue(paySumAmount);
		LTVInfo.setTimestamp(DCDateUtil.getStatDate(LTVInfo));
		dynamicKey.setDeWritable(LTVInfo);
		dynamicVal.setDeWritable(DENullWritable.get());
		// context.write(dynamicKey, dynamicVal);
		mos.write(dynamicKey, dynamicVal,
				StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ROLE_LTV));
	}
}
