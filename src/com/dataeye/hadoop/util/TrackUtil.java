package com.dataeye.hadoop.util;

import java.util.HashSet;
import java.util.Set;

import com.dataeye.hadoop.domain.common.MRConstants;

public class TrackUtil {

	/**
	 * <pre>
	 * 指定日期是否在偏移集合中
	 * @param offSet
	 * @param firstDate
	 * @param targetDate
	 * @return
	 * @author Hayden<br>
	 * @date 2015年11月6日 下午2:13:20
	 * <br>
	 */
	public static boolean isTrackOnByDay(Set<Integer> offSet, int firstDate, int targetDate) {
		if (offSet == null || offSet.isEmpty()) {
			return false;
		}
		int daysOffset = (targetDate - firstDate) / MRConstants.DAY_IN_SECONDS;
		return offSet.contains(daysOffset);
	}

	/**
	 * <pre>
	 * 判断与指定日期相隔指定天数的日期是否在偏移集合中
	 * @param offSet
	 * @param firstDate
	 * @param targetDate
	 * @param daysAgo
	 * @return
	 * @author Hayden<br>
	 * @date 2015年11月6日 下午2:13:45
	 * <br>
	 */
	public static boolean isTrackOnByDaysAgo(Set<Integer> offSet, int firstDate, int targetDate, int daysAgo) {
		if (offSet == null || offSet.isEmpty()) {
			return false;
		}
		int daysOffset = (targetDate - MRConstants.DAY_IN_SECONDS * daysAgo - firstDate) / MRConstants.DAY_IN_SECONDS;
		return offSet.contains(daysOffset);
	}

	/**
	 * <pre>
	 * 判断与指定日期相隔指定天数的日期是否有在偏移集合中
	 * @param offSet
	 * @param firstDate
	 * @param targetDate
	 * @param continueDays
	 * @return
	 * @author Hayden<br>
	 * @date 2015年11月6日 下午2:14:33
	 * <br>
	 */
	public static boolean isTrackOnByDaysEver(Set<Integer> offSet, int firstDate, int targetDate, int continueDays) {
		if (offSet == null || offSet.isEmpty()) {
			return false;
		}
		for (int i = 1; i <= continueDays; i++) {
			int daysOffset = (targetDate - MRConstants.DAY_IN_SECONDS * i - firstDate) / MRConstants.DAY_IN_SECONDS;
			if (offSet.contains(daysOffset)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * <pre>
	 * 指定日志之间是否有登录
	 * @param offSet
	 * @param firstDate
	 * @param targetDate
	 * @return
	 * @author Hayden<br>
	 * @date 2015年11月6日 下午2:13:20
	 * <br>
	 */
	public static boolean isTrackOnByDayBetween(Set<Integer> offSet, int firstDate, int startDate, int endDate) {
		if (offSet == null || offSet.isEmpty()) {
			return false;
		}
		for (int i = startDate; i < endDate; i += MRConstants.DAY_IN_SECONDS) {
			int daysOffset = (i - firstDate) / MRConstants.DAY_IN_SECONDS;
			if (offSet.contains(daysOffset)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * <pre>
	 * 指定日期是否在偏移集合中
	 * @param offSet
	 * @param firstDate
	 * @param targetDate
	 * @return
	 * @author Hayden<br>
	 * @date 2015年11月6日 下午2:13:20
	 * <br>
	 */
	public static boolean isTrackOnByDay(String trackJson, int firstDate, int targetDate) {
		Set<Integer> offSet = DCJsonUtil.jsonToIntegerSet(trackJson);
		return isTrackOnByDay(offSet, firstDate, targetDate);
	}

	/**
	 * <pre>
	 * 判断与指定日期相隔指定天数的日期是否在偏移集合中
	 * @param offSet
	 * @param firstDate
	 * @param targetDate
	 * @param daysAgo
	 * @return
	 * @author Hayden<br>
	 * @date 2015年11月6日 下午2:13:45
	 * <br>
	 */
	public static boolean isTrackOnByDaysAgo(String trackJson, int firstDate, int targetDate, int daysAgo) {
		Set<Integer> offSet = DCJsonUtil.jsonToIntegerSet(trackJson);
		return isTrackOnByDaysAgo(offSet, firstDate, targetDate, daysAgo);
	}

	/**
	 * <pre>
	 * 判断与指定日期相隔指定天数的日期是否有在偏移集合中
	 * @param offSet
	 * @param firstDate
	 * @param targetDate
	 * @param continueDays
	 * @return
	 * @author Hayden<br>
	 * @date 2015年11月6日 下午2:14:33
	 * <br>
	 */
	public static boolean isTrackOnByDaysEver(String trackJson, int firstDate, int targetDate, int continueDays) {
		Set<Integer> offSet = DCJsonUtil.jsonToIntegerSet(trackJson);
		return isTrackOnByDaysEver(offSet, firstDate, targetDate, continueDays);
	}

	/**
	 * <pre>
	 * 指定日志之间是否有登录
	 * @param offSet
	 * @param firstDate
	 * @param targetDate
	 * @return
	 * @author Hayden<br>
	 * @date 2015年11月6日 下午2:13:20
	 * <br>
	 */
	public static boolean isTrackOnByDayBetween(String trackJson, int firstDate, int startDate, int endDate) {
		Set<Integer> offSet = DCJsonUtil.jsonToIntegerSet(trackJson);
		return isTrackOnByDayBetween(offSet, firstDate, startDate, endDate);
	}

	/**
	 * <pre>
	 * 将指定日期制定到trackJson中
	 * @param track
	 * @param firstDate
	 * @param targetDate
	 * @return
	 * @author Hayden<br>
	 * @date 2015年11月7日 下午7:22:32
	 * <br>
	 */
	public static String addTargetDate2Track(String track, int firstDate, int targetDate) {
		Set<Integer> olDaysSet = DCJsonUtil.jsonToIntegerSet(track);
		if (null == olDaysSet) {
			olDaysSet = new HashSet<Integer>();
		}
		if (targetDate >= firstDate) {
			olDaysSet.add((targetDate - firstDate) / MRConstants.DAY_IN_SECONDS);
		}
		return DCJsonUtil.getGson().toJson(olDaysSet);
	}
}
