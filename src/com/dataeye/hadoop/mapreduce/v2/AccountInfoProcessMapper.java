package com.dataeye.hadoop.mapreduce.v2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.dataeye.hadoop.common.dewritable.DEDynamicKV;
import com.dataeye.hadoop.domain.common.DENullWritable;
import com.dataeye.hadoop.domain.common.MRConstants;
import com.dataeye.hadoop.domain.common.MRConstants.PathConstant;
import com.dataeye.hadoop.domain.kv.AccountRollInfo;
import com.dataeye.hadoop.domain.result.AccountBackInfo;
import com.dataeye.hadoop.domain.result.AccountDayInfo;
import com.dataeye.hadoop.domain.result.AccountLTVInfo;
import com.dataeye.hadoop.domain.result.AccountRetainInfo;
import com.dataeye.hadoop.util.DCDateUtil;
import com.dataeye.hadoop.util.StringUtil;
import com.dataeye.hadoop.util.TrackUtil;

public class AccountInfoProcessMapper extends Mapper<LongWritable, Text, DEDynamicKV, DEDynamicKV> {

	private DEDynamicKV dynamicKey = new DEDynamicKV();
	private DEDynamicKV dynamicVal = new DEDynamicKV();
	private MultipleOutputs<DEDynamicKV, DEDynamicKV> mos;

	private int dataDate = 0;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		dataDate = DCDateUtil.getDataDate(context.getConfiguration());
		mos = new MultipleOutputs<DEDynamicKV, DEDynamicKV>(context);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		AccountRollInfo info = new AccountRollInfo(value.toString().split(MRConstants.FIELDS_SEPARATOR));
		handleAccountDayInfo(info, context);
		handleAccountRetain(info, context);
		handleAccountBack(info, context);
		handleAccountLTV(info, context);
	}

	private void handleAccountDayInfo(AccountRollInfo rollInfo, Context context) throws IOException,
			InterruptedException {
		// 判断玩家各项信息
		AccountDayInfo dayInfo = new AccountDayInfo(rollInfo);
		if (rollInfo.getDayCreateTime() > 0 && 0 == rollInfo.getFirstLoginDate()) {
			dayInfo.setIsNewPlayer(MRConstants.INT_TRUE);
		}
		if (rollInfo.getDayPayAmount() > 0) {
			dayInfo.setIsPayPlayer(MRConstants.INT_TRUE);
		}
		if (0 == rollInfo.getFirstLoginDate() && rollInfo.getDayPayAmount() > 0) {
			dayInfo.setIsPayAtFirstDay(MRConstants.INT_TRUE);
		}
		if (rollInfo.getDayPayAmount() > 0 || rollInfo.getTotalPayTimes() > 0) {
			dayInfo.setIsEverPayPlayer(MRConstants.INT_TRUE);
		}
		if (dayInfo.getIsNewPlayer() > 0 && !StringUtil.isEmpty(rollInfo.getDayRoleIds())
				&& !MRConstants.STR_EMPTY_SET_JSON.equals(rollInfo.getDayRoleIds())) {
			dayInfo.setIsNewAndCreateRole(1);
		}

		dayInfo.setDayLoginTimes(rollInfo.getDayLoginTimes());
		dayInfo.setDayOnlineTime(rollInfo.getDayOnlineTime());
		dayInfo.setDayPayTimes(rollInfo.getDayPayTimes());
		dayInfo.setDayPayAmount(rollInfo.getDayPayAmount());

		if (rollInfo.getDayLoginTimes() > 0) {
			dayInfo.setTotalOnlineDays(1 + rollInfo.getTotalOnlineDays());
		} else {
			dayInfo.setTotalOnlineDays(rollInfo.getTotalOnlineDays());
		}
		dayInfo.setTotalLoginTimes(rollInfo.getDayLoginTimes() + rollInfo.getTotalLoginTimes());
		dayInfo.setTotalOnlineTime(rollInfo.getDayOnlineTime() + rollInfo.getTotalOnlineTime());
		dayInfo.setTotalPayTimes(rollInfo.getDayPayTimes() + rollInfo.getTotalPayTimes());
		dayInfo.setTotalPayAmount(rollInfo.getDayPayAmount() + rollInfo.getTotalPayAmount());

		// 输出最终结果前设置时间为根据时区修正过的日期
		dayInfo.setTimestamp(DCDateUtil.getStatDate(dayInfo));
		dynamicKey.setDeWritable(dayInfo);
		dynamicVal.setDeWritable(DENullWritable.get());
		// context.write(dynamicKey, dynamicVal);
		mos.write(dynamicKey, dynamicVal,
				StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ACCOUNT_INFO));
	}

	private void handleAccountRetain(AccountRollInfo rollInfo, Context context) throws IOException,
			InterruptedException {
		// 根据onlineTrack判断玩家留存信息
		AccountRetainInfo retainInfo = new AccountRetainInfo(rollInfo);
		// 输出最终结果前设置时间为根据时区修正过的日期
		retainInfo.setTimestamp(DCDateUtil.getStatDate(retainInfo));

		for (int i = 0; i < MRConstants.RETAIN_MAX_DAYS; i++) {
			int targetDate = dataDate - i * 24 * 3600;
			if (!TrackUtil.isTrackOnByDay(rollInfo.getOlTrack(), rollInfo.getFirstLoginDate(), targetDate)) {
				continue;
			}

			// 新增
			if (targetDate == rollInfo.getFirstLoginDate()) {
				retainInfo.setIsNewPlayer(MRConstants.INT_TRUE);
			}
			// 留存目标天是否有付费
			if (TrackUtil.isTrackOnByDay(rollInfo.getPayTrack(), rollInfo.getFirstLoginDate(), targetDate)) {
				retainInfo.setIsPayPlayer(MRConstants.INT_TRUE);
			}
			// 曾经付费
			if (rollInfo.getTotalPayTimes() > 0) {
				retainInfo.setIsEverPayPlayer(MRConstants.INT_TRUE);
			}

			retainInfo.setActionTimes(MRConstants.RETAIN_ACTION_TIMES);
			retainInfo.setTargetDate(targetDate);
			retainInfo.setnDayRetain(i + 1);
			dynamicKey.setDeWritable(retainInfo);
			dynamicVal.setDeWritable(DENullWritable.get());
			// context.write(dynamicKey, dynamicVal);
			mos.write(dynamicKey, dynamicVal,
					StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ACCOUNT_RETAIN));
		}
	}

	private void handleAccountBack(AccountRollInfo rollInfo, Context context) throws IOException, InterruptedException {
		// 根据onlineTrack判断玩家回流情况
		AccountBackInfo backInfo = new AccountBackInfo(rollInfo);
		// 输出最终结果前设置时间为根据时区修正过的日期
		backInfo.setTimestamp(DCDateUtil.getStatDate(backInfo));

		if (rollInfo.getLastLoginDate() <= 0) {
			return;
		}
		dynamicKey.setDeWritable(backInfo);
		dynamicVal.setDeWritable(DENullWritable.get());

		// 是否曾经付费
		if (rollInfo.getTotalPayTimes() > 0) {
			backInfo.setIsEverPay(MRConstants.INT_TRUE);
		}

		int nDayBack = (dataDate - rollInfo.getLastLoginDate()) / MRConstants.DAY_IN_SECONDS;
		if (nDayBack >= MRConstants.LOST_DAYS_3) {
			backInfo.setnDayBack(MRConstants.LOST_DAYS_3);
			// context.write(dynamicKey, dynamicVal);
			mos.write(dynamicKey, dynamicVal,
					StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ACCOUNT_BACK));
		}

		if (nDayBack >= MRConstants.LOST_DAYS_7) {
			backInfo.setnDayBack(MRConstants.LOST_DAYS_7);
			// context.write(dynamicKey, dynamicVal);
			mos.write(dynamicKey, dynamicVal,
					StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ACCOUNT_BACK));
		}

		if (nDayBack >= MRConstants.LOST_DAYS_14) {
			backInfo.setnDayBack(MRConstants.LOST_DAYS_14);
			// context.write(dynamicKey, dynamicVal);
			mos.write(dynamicKey, dynamicVal,
					StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ACCOUNT_BACK));
		}
	}

	private void handleAccountLTV(AccountRollInfo info, Context context) throws IOException, InterruptedException {
		float payAmount = info.getDayPayAmount() + info.getTotalPayAmount();
		int nDay = 0;
		int firstLoginDate = info.getFirstLoginDate();
		if (0 == info.getFirstLoginDate()) {
			nDay = 1;
			firstLoginDate = dataDate + MRConstants.DAY_IN_SECONDS;
		} else {
			nDay = (dataDate - info.getFirstLoginDate()) / MRConstants.DAY_IN_SECONDS + 2;
		}
		// 如果 payAmount 为 0 或这超过 LTV 统计周期则跳过
		if (payAmount <= 0 || nDay > MRConstants.LTV_MAX_DAYS) {
			return;
		}

		AccountLTVInfo ltvInfo = new AccountLTVInfo(info);
		ltvInfo.setFirstLoginDate(firstLoginDate);// 新增日期
		ltvInfo.setnDayLife(nDay);
		ltvInfo.setnDayValue(info.getTotalPayAmount() + info.getDayPayAmount());
		ltvInfo.setTimestamp(DCDateUtil.getStatDate(ltvInfo));
		dynamicKey.setDeWritable(ltvInfo);
		dynamicVal.setDeWritable(DENullWritable.get());
		// context.write(dynamicKey, dynamicVal);
		mos.write(dynamicKey, dynamicVal,
				StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ACCOUNT_LTV));
	}

}
