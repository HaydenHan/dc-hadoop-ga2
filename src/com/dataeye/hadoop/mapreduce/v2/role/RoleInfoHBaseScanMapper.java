package com.dataeye.hadoop.mapreduce.v2.role;

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
import com.dataeye.hadoop.domain.hbase.RoleHistoryInfo;
import com.dataeye.hadoop.domain.result.role.RoleLTVInfo;
import com.dataeye.hadoop.domain.result.role.RoleLostInfo;
import com.dataeye.hadoop.domain.result.role.RoleRetainInfo;
import com.dataeye.hadoop.util.DCDateUtil;
import com.dataeye.hadoop.util.HBaseUtil;
import com.dataeye.hadoop.util.RoleCacheUtil;
import com.dataeye.hadoop.util.StringUtil;
import com.dataeye.hadoop.util.TrackUtil;

/**
 * <pre>
 * 冷数据计算流失，计算周月指标
 * @author Hayden<br>
 * @date 2015年11月7日 下午2:36:06
 * <br>
 */
public class RoleInfoHBaseScanMapper extends TableMapper<DEDynamicKV, DEDynamicKV> {

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

	protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException,
			InterruptedException {
		RoleHistoryInfo histInfo = RoleCacheUtil.wrapRoleHistoryInfo(value);
		dynamicKey.setDeWritable(histInfo);
		dynamicVal.setDeWritable(DENullWritable.get());
		context.write(dynamicKey, dynamicVal);
		// 角色流失
		handleRoleLost(histInfo, context);
		// 角色周LTV
		handleRoleWeekLTV(histInfo, context);
		// 角色月LTV
		handleRoleMonthLTV(histInfo, context);
		// 角色周留存
		handleRoleWeekRetain(histInfo, context);
		// 角色月留存
		handleRoleMonthRetain(histInfo, context);
	}

	/**
	 * <pre>
	 * 角色流失
	 * @param histInfo
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 * @author Hayden<br>
	 * @date 2015年11月6日 下午7:59:36
	 * <br>
	 */
	private void handleRoleLost(RoleHistoryInfo histInfo, Context context) throws IOException, InterruptedException {

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

		RoleLostInfo lostInfo = new RoleLostInfo(histInfo);
		if (histInfo.getTotalPayAmount() > 0) {
			lostInfo.setIsEverPay(MRConstants.INT_TRUE);
		}
		lostInfo.setLastLoginDate(histInfo.getLastLoginDate());
		lostInfo.setnDayLost(nDayLost);
		lostInfo.setTotalOnlineDays(histInfo.getTotalOnlineDays());
		lostInfo.setTotalOnlineTime(histInfo.getTotalOnlineTime());
		lostInfo.setTotalLoginTimes(histInfo.getTotalLoginTimes());

		lostInfo.setTimestamp(DCDateUtil.getStatDateByTsAndTz(dataDate, histInfo.getTimeZone()));
		dynamicKey.setDeWritable(lostInfo);
		dynamicVal.setDeWritable(DENullWritable.get());
		mos.write(dynamicKey, dynamicVal,
				StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ROLE_LOST));
	}

	/**
	 * <pre>
	 * 角色周LTV
	 * @param histInfo
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 * @author Hayden<br>
	 * @date 2015年11月6日 下午8:19:46
	 * <br>
	 */
	private void handleRoleWeekLTV(RoleHistoryInfo histInfo, Context context) throws IOException, InterruptedException {
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

		RoleLTVInfo ltvInfo = new RoleLTVInfo(histInfo);
		ltvInfo.setFirstLoginDate(mondayOfWeek);// 新增周的第一天
		ltvInfo.setnDayLife(weeks);
		ltvInfo.setnDayValue(histInfo.getTotalPayAmount());

		ltvInfo.setTimestamp(DCDateUtil.getStatDateByTsAndTz(dataDate - 6 * MRConstants.DAY_IN_SECONDS,
				histInfo.getTimeZone()));
		dynamicKey.setDeWritable(ltvInfo);
		dynamicVal.setDeWritable(DENullWritable.get());
		mos.write(dynamicKey, dynamicVal,
				StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ROLE_WEEK_LTV));
	}

	/**
	 * <pre>
	 * 角色月LTV
	 * @param histInfo
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 * @author Hayden<br>
	 * @date 2015年11月6日 下午8:25:15
	 * <br>
	 */
	private void handleRoleMonthLTV(RoleHistoryInfo histInfo, Context context) throws IOException, InterruptedException {
		if (histInfo.getTotalPayAmount() <= 0) {
			return;
		}

		// 当调度时间为某月 1 号时，结算上一月的信息
		int scheduleTime = dataDate + MRConstants.DAY_IN_SECONDS;
		cal.setTimeInMillis(1000L * scheduleTime);
		if (1 != cal.get(Calendar.DAY_OF_MONTH)) {
			return;
		}

		int firstDayOfMonth = DCDateUtil.getFirstDayOfMonth(histInfo.getFirstLoginDate());
		// 新增第一月时 nWeek 为 0，所以要+1
		int month = 1 + DCDateUtil.getMonthBetween(dataDate, histInfo.getFirstLoginDate());
		if (month > MRConstants.LTV_MAX_MONTH) {
			return;
		}

		RoleLTVInfo ltvInfo = new RoleLTVInfo(histInfo);
		ltvInfo.setFirstLoginDate(firstDayOfMonth);// 新增月的第一天
		ltvInfo.setnDayLife(month);
		ltvInfo.setnDayValue(histInfo.getTotalPayAmount());

		cal.add(Calendar.MONTH, -1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		ltvInfo.setTimestamp(DCDateUtil.getStatDateByTsAndTz(cal.getTimeInMillis() / 1000, histInfo.getTimeZone()));
		dynamicKey.setDeWritable(ltvInfo);
		dynamicVal.setDeWritable(DENullWritable.get());
		mos.write(dynamicKey, dynamicVal,
				StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ROLE_MONTH_LTV));
	}

	/**
	 * <pre>
	 * 角色周留存
	 * @param histInfo
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 * @author Hayden<br>
	 * @date 2015年11月6日 下午8:27:56
	 * <br>
	 */
	private void handleRoleWeekRetain(RoleHistoryInfo histInfo, Context context) throws IOException,
			InterruptedException {
		// 在数据结算中约定一周的开始和结束分别为周一和周日
		// 所以当调度时间为周一时，结算上一周的信息
		int scheduleTime = dataDate + MRConstants.DAY_IN_SECONDS;
		cal.setTimeInMillis(1000L * scheduleTime);
		if (Calendar.MONDAY != cal.get(Calendar.DAY_OF_WEEK)) {
			return;
		}
		RoleRetainInfo retainInfo = new RoleRetainInfo(histInfo);

		for (int i = 1; i <= MRConstants.RETAIN_MAX_WEEKS; i++) {
			int endDate = (int) (cal.getTimeInMillis() / 1000);
			cal.add(Calendar.DAY_OF_MONTH, -7);
			int startDate = (int) (cal.getTimeInMillis() / 1000);
			if (endDate <= histInfo.getFirstLoginDate()) {
				return;
			}
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
				retainInfo.setIsNewRole(MRConstants.INT_TRUE);
			}
			// 曾经付费
			if (histInfo.getTotalPayTimes() > 0) {
				retainInfo.setIsPayRole(MRConstants.INT_TRUE);
			}
			// 留存目标天是否有付费
			if (TrackUtil.isTrackOnByDayBetween(histInfo.getPayTrack(), histInfo.getFirstPayDate(), startDate, endDate)) {
				retainInfo.setIsEverPayRole(MRConstants.INT_TRUE);
			}
			retainInfo.setActionTimes(1);
			retainInfo.setTargetDate(startDate);
			retainInfo.setnDayRetain(i - 1);
			retainInfo.setTimestamp(DCDateUtil.getStatDateByTsAndTz(dataDate - 6 * MRConstants.DAY_IN_SECONDS,
					histInfo.getTimeZone()));
			dynamicKey.setDeWritable(retainInfo);
			dynamicVal.setDeWritable(DENullWritable.get());
			mos.write(dynamicKey, dynamicVal,
					StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ROLE_WEEK_RETAIN));
		}
	}

	/**
	 * <pre>
	 * 角色月留存
	 * @param histInfo
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 * @author Hayden<br>
	 * @date 2015年11月6日 下午8:28:15
	 * <br>
	 */
	private void handleRoleMonthRetain(RoleHistoryInfo histInfo, Context context) throws IOException,
			InterruptedException {
		// 当调度时间为某月 1 号时，结算上一月的信息
		int scheduleTime = dataDate + MRConstants.DAY_IN_SECONDS;
		cal.setTimeInMillis(1000L * scheduleTime);
		if (1 != cal.get(Calendar.DAY_OF_MONTH)) {
			return;
		}

		RoleRetainInfo retainInfo = new RoleRetainInfo(histInfo);
		cal.add(Calendar.MONTH, -1);
		retainInfo.setTimestamp(DCDateUtil.getStatDateByTsAndTz(cal.getTimeInMillis() / 1000, histInfo.getTimeZone()));
		cal.add(Calendar.MONTH, 1);

		for (int i = 1; i <= MRConstants.RETAIN_MAX_MONTH; i++) {
			int endDate = (int) (cal.getTimeInMillis() / 1000);
			cal.add(Calendar.MONTH, -1);
			int startDate = (int) (cal.getTimeInMillis() / 1000);
			boolean isEverLogin = TrackUtil.isTrackOnByDayBetween(histInfo.getOlTrack(), histInfo.getFirstLoginDate(),
					startDate, endDate);
			if (!isEverLogin) {
				if (i == 1) {
					return;
				} else {
					continue;
				}
			}

			// 新增
			if (startDate <= histInfo.getFirstLoginDate() && histInfo.getFirstLoginDate() < endDate) {
				retainInfo.setIsNewRole(MRConstants.INT_TRUE);
			}
			// 曾经付费
			if (histInfo.getTotalPayTimes() > 0) {
				retainInfo.setIsEverPayRole(MRConstants.INT_TRUE);
			}
			// 留存目标天是否有付费
			if (TrackUtil.isTrackOnByDayBetween(histInfo.getPayTrack(), histInfo.getFirstPayDate(), startDate, endDate)) {
				retainInfo.setIsPayRole(MRConstants.INT_TRUE);
			}

			retainInfo.setActionTimes(1);
			retainInfo.setTargetDate(startDate);
			retainInfo.setnDayRetain(i - 1);

			dynamicKey.setDeWritable(retainInfo);
			dynamicVal.setDeWritable(DENullWritable.get());
			mos.write(dynamicKey, dynamicVal,
					StringUtil.getOutPutPath(context.getConfiguration(), PathConstant.BUSINESS_ROLE_MONTH_RETAIN));
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
}
