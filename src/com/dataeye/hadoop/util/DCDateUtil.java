package com.dataeye.hadoop.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;

import com.dataeye.hadoop.domain.common.DEHeaderWritable;
import com.dataeye.hadoop.domain.common.MRConstants;

public class DCDateUtil {
	private final static SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
	private final static SimpleDateFormat yyyyMMddHH = new SimpleDateFormat("yyyy-MM-dd-HH");
	private final static SimpleDateFormat yyyyMMddHHmm = new SimpleDateFormat("yyyyMMddHHmm");
	private final static SimpleDateFormat yyyyMMddHHmmss = new SimpleDateFormat("yyyyMMddHHmmss");
	private final static SimpleDateFormat yyyy_MM_ddHHmmss = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private final static SimpleDateFormat yyyy_MM_dd = new SimpleDateFormat("yyyy-MM-dd");

	public static int getMondayOfSWeek(int time){
		return (int)(shift2MondayTime(1000L * time)/1000);
	}
	
	public static int getWeeksBetween(int startTime, int endTime){
		long day1 = shift2MondayTime(1000L*startTime);
		long day2 = shift2MondayTime(1000L*endTime);
		return Math.abs((int)((day2 - day1)/MRConstants.WEEK_IN_SECONDS));
	}
	
	public static int getMonthBetween(int startTime, int endTime) {
        return getMonthBetween(1000L * startTime, 1000L * endTime);
    }
	
	public static int getMonthBetween(long startTime, long endTime) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(startTime);
        int month1 = c.get(Calendar.MONTH);
        int year1 = c.get(Calendar.YEAR);
        
        c.setTimeInMillis(endTime);
        int month2 = c.get(Calendar.MONTH);
        int year2 = c.get(Calendar.YEAR);
        return Math.abs((year2 - year1) * 12 + (month2 - month1));
    }
	
	public static int getFirstDayOfMonth(int time){
		return (int)(getFirstDayOfMonth(1000L * time));
	}
	
	public static long getFirstDayOfMonth(long time){
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(time);
		
		return shift2FirstDayOfMonth(cal);
	}
	
	private static long shift2FirstDayOfMonth(Calendar cal){
		cal.set(Calendar.DAY_OF_MONTH, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTimeInMillis();
	}
	
	private static long shift2MondayTime(long time){
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(time);
		
		return shift2MondayTime(cal);
	}
	
	private static long shift2MondayTime(Calendar cal){
		cal.setFirstDayOfWeek(Calendar.MONDAY);
		cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		
		return cal.getTimeInMillis();
	}
	

	public static String date2yyyyMMdd(long timemilles) {
		return yyyyMMdd.format(new Date(timemilles));
	}

	public static int date2yyyyMMddInt(long timemilles) {
		return StringUtil.convertInt(date2yyyyMMdd(timemilles), 0);
	}

	public static String date2yyyyMMddHHmm(long timemilles) {
		return yyyyMMddHHmm.format(new Date(timemilles));
	}

	public static Date yyyyMMddHH2Date(String str) throws ParseException {
		return yyyyMMddHH.parse(str);
	}

	public static String date2yyyyMMddHH(long timemilles) {
		return yyyyMMddHH.format(new Date(timemilles));
	}

	public static Date yyyy_MM_ddHHmmss2Date(String str) throws ParseException {
		return yyyy_MM_ddHHmmss.parse(str);
	}

	public static String date2yyyy_MM_ddHHmmss(Date date) {
		return yyyy_MM_ddHHmmss.format(date);
	}

	public static String date_yyyyMMddHHmmss(long timemilles) {
		return yyyyMMddHHmmss.format(new Date(timemilles));
	}

	public static String date2yyyy_MM_dd(long timemilles) {
		return yyyy_MM_dd.format(new Date(timemilles));
	}

	public static int getStatDateByTsAndTz(long timestamp, String timeZero) {
		// @TODO 时区处理
		return date2yyyyMMddInt(timestamp * 1000);
	}
	
	public static int getStatDate(DEHeaderWritable deHeader) {
		return getStatDateByTsAndTz(deHeader.getTimestamp(), deHeader.getTimeZone());
	}

	public static int getDataDate(Configuration conf) {
		String scheduleTime = conf.get("job.schedule.time");
		System.out.println("scheduleTime:" + scheduleTime);
		if (!StringUtil.isEmpty(scheduleTime)) {
			Date date = null;
			try {
				date = yyyy_MM_ddHHmmss2Date(scheduleTime);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			Calendar calendar = Calendar.getInstance();
			if (date != null) {
				calendar.setTime(date);
			}
			calendar.add(Calendar.DAY_OF_MONTH, -1);
			calendar.set(Calendar.HOUR_OF_DAY, 0);
			calendar.set(Calendar.MINUTE, 0);
			calendar.set(Calendar.SECOND, 0);
			return (int) (calendar.getTimeInMillis() / 1000);
		}
		return 0;
	}

	public static int getDataDate23Hour(Configuration conf) {
		String scheduleTime = conf.get("job.schedule.time");
		System.out.println("scheduleTime:" + scheduleTime);
		if (!StringUtil.isEmpty(scheduleTime)) {
			Date date = null;
			try {
				date = yyyy_MM_ddHHmmss2Date(scheduleTime);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			Calendar calendar = Calendar.getInstance();
			if (date != null) {
				calendar.setTime(date);
			}
			calendar.add(Calendar.DAY_OF_MONTH, -1);
			calendar.set(Calendar.HOUR_OF_DAY, 23);
			calendar.set(Calendar.MINUTE, 0);
			calendar.set(Calendar.SECOND, 0);
			return (int) (calendar.getTimeInMillis() / 1000);
		}
		return 0;
	}

	public static long getLastHourDate(Configuration conf) {
		String scheduleTime = conf.get("job.schedule.time");
		System.out.println("scheduleTime:" + scheduleTime);
		if (!StringUtil.isEmpty(scheduleTime)) {
			Date date = null;
			try {
				date = yyyy_MM_ddHHmmss2Date(scheduleTime);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			Calendar calendar = Calendar.getInstance();
			if (date != null) {
				calendar.setTime(date);
			}
			calendar.add(Calendar.HOUR_OF_DAY, -1);
			calendar.set(Calendar.MINUTE, 0);
			calendar.set(Calendar.SECOND, 0);
			return (int) (calendar.getTimeInMillis() / 1000);
		}
		return 0;
	}

	public static boolean isNeedLastHourRolling(Configuration conf) {
		String scheduleTime = conf.get("job.schedule.time");
		int firstHour = conf.getInt("dc.first.job.hour", 1);
		if (!StringUtil.isEmpty(scheduleTime)) {
			Date date = null;
			try {
				date = yyyy_MM_ddHHmmss2Date(scheduleTime);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			Calendar calendar = Calendar.getInstance();
			if (date != null) {
				calendar.setTimeInMillis(date.getTime());
			}
			return firstHour != calendar.get(Calendar.HOUR_OF_DAY);
		}
		return true;
	}

	public static void main(String[] args) throws Exception {
		Date date = null;
		try {
			date = yyyy_MM_ddHHmmss2Date("2015-11-18 01:30:50");
		} catch (ParseException e) {
			e.printStackTrace();
		}
		Calendar calendar = Calendar.getInstance();
		if (date != null) {
			calendar.setTimeInMillis(date.getTime());
		}
		System.out.println(calendar.get(Calendar.HOUR_OF_DAY));
	}
}
