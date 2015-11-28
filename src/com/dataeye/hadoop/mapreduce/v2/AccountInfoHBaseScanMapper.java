package com.dataeye.hadoop.mapreduce.v2;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.dataeye.hadoop.common.dewritable.DEDynamicKV;
import com.dataeye.hadoop.domain.common.DENullWritable;
import com.dataeye.hadoop.domain.common.MRConstants;
import com.dataeye.hadoop.domain.common.MRConstants.PathConstant;
import com.dataeye.hadoop.domain.hbase.AccountHistoryInfo;
import com.dataeye.hadoop.domain.result.AccountLTVInfo;
import com.dataeye.hadoop.domain.result.AccountLostInfo;
import com.dataeye.hadoop.domain.result.AccountRetainInfo;
import com.dataeye.hadoop.util.AccountCacheUtil;
import com.dataeye.hadoop.util.DCDateUtil;
import com.dataeye.hadoop.util.HBaseUtil;
import com.dataeye.hadoop.util.StringUtil;
import com.dataeye.hadoop.util.TrackUtil;

public class AccountInfoHBaseScanMapper extends TableMapper<DEDynamicKV, DEDynamicKV> {

	private DEDynamicKV dynamicKey = new DEDynamicKV();
	private DEDynamicKV dynamicVal = new DEDynamicKV();
	private MultipleOutputs<DEDynamicKV, DEDynamicKV> mos;

	private int dataDate = 0;
	private Calendar cal = Calendar.getInstance();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		dataDate = DCDateUtil.getDataDate(context.getConfiguration());
		mos = new MultipleOutputs<DEDynamicKV, DEDynamicKV>(context);
		HBaseUtil.init(context.getConfiguration());
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}

	protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException,
			InterruptedException {
		AccountHistoryInfo histInfo = AccountCacheUtil.wrapAccountHistoryInfo(value);
		dynamicKey.setDeWritable(histInfo);
		dynamicVal.setDeWritable(DENullWritable.get());
		context.write(dynamicKey, dynamicVal);
		// 设置来源滚服信息
		AccountCacheUtil.getAndSetFromGS(histInfo, dataDate);
		handleAccountLost(histInfo, context);
		handleAccWeekLTV(histInfo, context);
		handleAccMonthLTV(histInfo, context);
		handleAccWeekRetain(histInfo, context);
		handleAccMonthRetain(histInfo, context);
	}

	private void handleAccountLost(AccountHistoryInfo histInfo, Context context) throws IOException,
			InterruptedException {

		// 统计 3/7/14 流失
		int daysAgo_3 = dataDate - MRConstants.LOST_DAYS_3 * MRConstants.DAY_IN_SECONDS;
		int daysAgo_7 = dataDate - MRConstants.LOST_DAYS_7 * MRConstants.DAY_IN_SECONDS;
		int daysAgo_14 = dataDate - MRConstants.LOST_DAYS_14 * MRConstants.DAY_IN_SECONDS;
		int nDayLost = 0;
		int targetDate = 0;
		String onlineTrack = histInfo.getOlTrack();
		int firstLoginDate = histInfo.getFirstLoginDate();
		if (TrackUtil.isTrackOnByDay(onlineTrack, firstLoginDate, daysAgo_3)
				&& !TrackUtil.isTrackOnByDaysEver(onlineTrack, firstLoginDate, daysAgo_3, MRConstants.LOST_DAYS_3)) {
			nDayLost = 3;
			targetDate = daysAgo_3;
		} else if (TrackUtil.isTrackOnByDay(onlineTrack, firstLoginDate, daysAgo_7)
				&& !TrackUtil.isTrackOnByDaysEver(onlineTrack, firstLoginDate, daysAgo_7, MRConstants.LOST_DAYS_7)) {
			nDayLost = 7;
			targetDate = daysAgo_7;
		} else if (TrackUtil.isTrackOnByDay(onlineTrack, firstLoginDate, daysAgo_14)
				&& !TrackUtil.isTrackOnByDaysEver(onlineTrack, firstLoginDate, daysAgo_14, MRConstants.LOST_DAYS_14)) {
			nDayLost = 14;
			targetDate = daysAgo_14;
		}

		if (0 == targetDate || 0 == nDayLost) {
			return;
		}

		AccountLostInfo lostInfo = new AccountLostInfo(histInfo);
		if (histInfo.getTotalPayTimes() > 0) {
			lostInfo.setIsEverPayPlayer(MRConstants.INT_TRUE);
		}
		lostInfo.setLastLogDate(histInfo.getLastLoginDate());
		lostInfo.setnDayLost(nDayLost);
		lostInfo.setTotalOnlineDays(histInfo.getTotalOnlineDays());
		lostInfo.setTotalOnlineTime(histInfo.getTotalOnlineTime());
		lostInfo.setTotalLoginTimes(histInfo.getTotalLoginTimes());

		// 输出最终结果前设置时间为根据时区修正过的日期
		// lostInfo.setTimestamp(DCDateUtil.getStatDate(lostInfo));
		lostInfo.setTimestamp(DCDateUtil.getStatDateByTsAndTz(dataDate, histInfo.getTimeZone()));
		dynamicKey.setDeWritable(lostInfo);
		dynamicVal.setDeWritable(DENullWritable.get());
		// context.write(dynamicKey, dynamicVal);
		mos.write(dynamicKey, dynamicVal,
				StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ACCOUNT_LOST));
	}

	private void handleAccWeekLTV(AccountHistoryInfo histInfo, Context context) throws IOException,
			InterruptedException {
		if (histInfo.getTotalPayAmount() <= 0) {
			return;
		}

		// 在数据结算中约定一周的开始和结束分别为周一和周日
		// 所以当调度时间为周一时，结算上一周的信息
		int scheduleTime = dataDate + MRConstants.DAY_IN_SECONDS;
		cal.setTimeInMillis(1000L * scheduleTime);
		if (Calendar.MONDAY != cal.get(Calendar.DAY_OF_WEEK)) {
			return;
		}

		int mondayOfWeek = DCDateUtil.getMondayOfSWeek(histInfo.getFirstLoginDate());
		// 新增第一周时 weeks 为 0，所以要+1
		int weeks = 1 + DCDateUtil.getWeeksBetween(dataDate - 6 * MRConstants.DAY_IN_SECONDS,
				histInfo.getFirstLoginDate());
		if (weeks > MRConstants.LTV_MAX_WEEKS) {
			return;
		}

		AccountLTVInfo ltvInfo = new AccountLTVInfo(histInfo);
		ltvInfo.setFirstLoginDate(mondayOfWeek);// 新增周的第一天
		ltvInfo.setnDayLife(weeks);
		ltvInfo.setnDayValue(histInfo.getTotalPayAmount());

		// 输出最终结果前设置时间为根据时区修正过的日期
		ltvInfo.setTimestamp(DCDateUtil.getStatDateByTsAndTz(dataDate - 6 * MRConstants.DAY_IN_SECONDS,
				histInfo.getTimeZone()));

		dynamicKey.setDeWritable(ltvInfo);
		dynamicVal.setDeWritable(DENullWritable.get());
		// context.write(dynamicKey, dynamicVal);
		mos.write(dynamicKey, dynamicVal,
				StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ACCOUNT_WEEK_LTV));
	}

	private void handleAccMonthLTV(AccountHistoryInfo histInfo, Context context) throws IOException,
			InterruptedException {
		if (histInfo.getTotalPayAmount() <= 0) {
			return;
		}

		// 当调度时间为某月 1 号时，结算上一月的信息
		int scheduleTime = dataDate + MRConstants.DAY_IN_SECONDS;
		cal.setTimeInMillis(1000L * scheduleTime);
		if (1 != cal.get(Calendar.DAY_OF_MONTH)) {
			return;
		}

		// int firstDayOfMonth = DCDateUtil.getFirstDayOfMonth(histInfo.getFirstLoginDate());
		// 新增第一月时 nWeek 为 0，所以要+1
		int month = 1 + DCDateUtil.getMonthBetween(dataDate, histInfo.getFirstLoginDate());
		if (month > MRConstants.LTV_MAX_MONTH) {
			return;
		}

		AccountLTVInfo ltvInfo = new AccountLTVInfo(histInfo);
		ltvInfo.setFirstLoginDate(histInfo.getFirstLoginDate());// 新增月的第一天
		ltvInfo.setnDayLife(month);
		ltvInfo.setnDayValue(histInfo.getTotalPayAmount());

		// 输出最终结果前设置时间为根据时区修正过的日期
		// ltvInfo.setTimestamp(DCDateUtil.getStatDate(ltvInfo));
		cal.add(Calendar.MONTH, -1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		ltvInfo.setTimestamp(DCDateUtil.getStatDateByTsAndTz(cal.getTimeInMillis() / 1000, histInfo.getTimeZone()));

		dynamicKey.setDeWritable(ltvInfo);
		dynamicVal.setDeWritable(DENullWritable.get());
		// context.write(dynamicKey, dynamicVal);
		mos.write(dynamicKey, dynamicVal,
				StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ACCOUNT_MONTH_LTV));
	}

	private void handleAccWeekRetain(AccountHistoryInfo histInfo, Context context) throws IOException,
			InterruptedException {
		// 在数据结算中约定一周的开始和结束分别为周一和周日
		// 所以当调度时间为周一时，结算上一周的信息
		int scheduleTime = dataDate + MRConstants.DAY_IN_SECONDS;
		cal.setTimeInMillis(1000L * scheduleTime);
		if (Calendar.MONDAY != cal.get(Calendar.DAY_OF_WEEK)) {
			return;
		}

		AccountRetainInfo retainInfo = new AccountRetainInfo(histInfo);

		for (int i = 1; i <= MRConstants.RETAIN_MAX_WEEKS; i++) {
			int endDate = (int) (cal.getTimeInMillis() / 1000);
			cal.add(Calendar.DAY_OF_MONTH, -7);
			int startDate = (int) (cal.getTimeInMillis() / 1000);
			boolean isEverLogin = TrackUtil.isTrackOnByDayBetween(histInfo.getOlTrack(), histInfo.getFirstLoginDate(),
					startDate, endDate);
			if (1 == i) {
				// i = 0 标识结算当周，如果当周没登录过则无所谓留存
				if (!isEverLogin) {
					break;
				}
				continue;
			} else if (!isEverLogin) {
				continue;
			}

			// 新增
			if (startDate <= histInfo.getFirstLoginDate() && histInfo.getFirstLoginDate() < endDate) {
				retainInfo.setIsNewPlayer(MRConstants.INT_TRUE);
			}
			// 曾经付费
			if (histInfo.getTotalPayTimes() > 0) {
				retainInfo.setIsEverPayPlayer(MRConstants.INT_TRUE);
			}
			// 留存目标天是否有付费
			if (TrackUtil.isTrackOnByDayBetween(histInfo.getPayTrack(), histInfo.getFirstPayDate(), startDate, endDate)) {
				retainInfo.setIsPayPlayer(MRConstants.INT_TRUE);
			}

			retainInfo.setActionTimes(MRConstants.RETAIN_ACTION_TIMES);
			retainInfo.setTargetDate(startDate);
			retainInfo.setnDayRetain(i - 1);

			// 输出最终结果前设置时间为根据时区修正过的日期
			// retainInfo.setTimestamp(DCDateUtil.getStatDate(retainInfo));
			retainInfo.setTimestamp(DCDateUtil.getStatDateByTsAndTz(dataDate - 6 * MRConstants.DAY_IN_SECONDS,
					histInfo.getTimeZone()));

			dynamicKey.setDeWritable(retainInfo);
			dynamicVal.setDeWritable(DENullWritable.get());
			// context.write(dynamicKey, dynamicVal);
			mos.write(dynamicKey, dynamicVal,
					StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ACCOUNT_WEEK_RETAIN));
		}
	}

	private void handleAccMonthRetain(AccountHistoryInfo histInfo, Context context) throws IOException,
			InterruptedException {
		// 当调度时间为某月 1 号时，结算上一月的信息
		int scheduleTime = dataDate + MRConstants.DAY_IN_SECONDS;
		cal.setTimeInMillis(1000L * scheduleTime);
		if (1 != cal.get(Calendar.DAY_OF_MONTH)) {
			return;
		}
		AccountRetainInfo retainInfo = new AccountRetainInfo(histInfo);
		// 输出最终结果前设置时间为根据时区修正过的日期
		cal.add(Calendar.MONTH, -1);
		retainInfo.setTimestamp(DCDateUtil.getStatDateByTsAndTz(cal.getTimeInMillis() / 1000, histInfo.getTimeZone()));
		cal.add(Calendar.MONTH, 1);

		for (int i = 0; i <= MRConstants.RETAIN_MAX_MONTH; i++) {

			int endDate = (int) (cal.getTimeInMillis() / 1000);
			cal.add(Calendar.MONTH, -1);
			int startDate = (int) (cal.getTimeInMillis() / 1000);
			boolean isEverLogin = TrackUtil.isTrackOnByDayBetween(histInfo.getOlTrack(), histInfo.getFirstLoginDate(),
					startDate, endDate);
			if (0 == i) {
				// i = 0 标识结算当月，如果当月没登录过则无所谓留存
				if (!isEverLogin) {
					break;
				}
				continue;
			} else if (!isEverLogin) {
				continue;
			}

			// 新增
			if (startDate <= histInfo.getFirstLoginDate() && histInfo.getFirstLoginDate() < endDate) {
				retainInfo.setIsNewPlayer(MRConstants.INT_TRUE);
			}
			// 曾经付费
			if (histInfo.getTotalPayTimes() > 0) {
				retainInfo.setIsEverPayPlayer(MRConstants.INT_TRUE);
			}
			// 留存目标天是否有付费
			if (TrackUtil.isTrackOnByDayBetween(histInfo.getPayTrack(), histInfo.getFirstPayDate(), startDate, endDate)) {
				retainInfo.setIsPayPlayer(MRConstants.INT_TRUE);
			}

			retainInfo.setActionTimes(MRConstants.RETAIN_ACTION_TIMES);
			retainInfo.setTargetDate(startDate);
			retainInfo.setnDayRetain(i - 1);

			dynamicKey.setDeWritable(retainInfo);
			dynamicVal.setDeWritable(DENullWritable.get());
			// context.write(dynamicKey, dynamicVal);
			mos.write(dynamicKey, dynamicVal,
					StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ACCOUNT_MONTH_RETAIN));
		}
	}
}
