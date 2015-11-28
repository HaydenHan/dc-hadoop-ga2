package com.dataeye.hadoop.util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import com.dataeye.hadoop.domain.common.MRConstants.PathConstant;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonParser;

public class StringUtil {
	public static Gson gson = new Gson();

	public static JsonParser jsonParser = new JsonParser();

	public static final Calendar calendar = Calendar.getInstance(); // 多线程使用不安全

	private static final int UNIXTIME_20130701 = 1372608000;

	public static int convertInt(String intStr, int defaultValue) {
		try {
			return Integer.valueOf(intStr).intValue();
		} catch (Throwable t1) {
			try {
				return Float.valueOf(intStr).intValue();
			} catch (Throwable t2) {
				return defaultValue;
			}
		}
	}

	public static long convertLong(String longStr, long defaultValue) {
		try {
			return Long.valueOf(longStr);
		} catch (Throwable t) {
			return defaultValue;
		}
	}

	public static float convertFloat(String floatStr, float defaultValue) {
		try {
			return Float.valueOf(floatStr);
		} catch (Throwable t) {
			return defaultValue;
		}
	}

	public static double convertDouble(String doubleStr, double defaultValue) {
		try {
			return Double.valueOf(doubleStr);
		} catch (Throwable t) {
			return defaultValue;
		}
	}

	public static BigDecimal convertBigDecimal(String bigDecimalDStr, int defaultValue) {
		try {
			return new BigDecimal(bigDecimalDStr);
		} catch (Throwable t) {
			return new BigDecimal(defaultValue);
		}
	}

	/**
	 * 分割字符串
	 * 
	 * @param line
	 *            原始字符串
	 * @param seperator
	 *            分隔符
	 * @return 分割结果
	 */
	public static String[] split(String line, String seperator) {
		if (line == null || seperator == null || seperator.length() == 0)
			return null;
		ArrayList<String> list = new ArrayList<String>();
		int pos1 = 0;
		int pos2;
		for (;;) {
			pos2 = line.indexOf(seperator, pos1);
			if (pos2 < 0) {
				list.add(line.substring(pos1));
				break;
			}
			list.add(line.substring(pos1, pos2));
			pos1 = pos2 + seperator.length();
		}
		// 去掉末尾的空串，和String.split行为保持一致
		for (int i = list.size() - 1; i >= 0 && list.get(i).length() == 0; --i) {
			list.remove(i);
		}
		return list.toArray(new String[0]);
	}

	public static boolean isEmpty(String str) {
		if (null == str || "".equals(str.trim())) {
			return true;
		}
		return false;
	}

	public static String getJsonStr(Object o) {
		String str = gson.toJson(o);
		return str;
	}

	public static Map<String, String> getMapFromJson(String json) {
		try {
			return gson.fromJson(json, new TypeToken<Map<String, String>>() {
			}.getType());
		} catch (Throwable t) {
			return null;
		}
	}

	public static Set<String> getSetFromJson(String json) {
		try {
			return gson.fromJson(json, HashSet.class);
		} catch (Throwable t) {
			return null;
		}
	}

	/**
	 * Added by rickpan
	 * 
	 * @param json
	 * @param type
	 * @return
	 */
	public static <T1, T2> Map<T1, T2> getMapFromJson(String json, TypeToken<Map<T1, T2>> type) {
		try {
			return gson.fromJson(json, type.getType());
		} catch (Throwable t) {
			return null;
		}
	}

	public static <T1, T2> Map<T1, T2> getMapFromJson(String json, T1 O, T2 P) {
		try {
			return gson.fromJson(json, new TypeToken<Map<T1, T2>>() {
			}.getType());
		} catch (Throwable t) {
			return null;
		}
	}

	/**
	 * <pre>
	 * 空字符串转换为默认值
	 * @date 2015年4月23日 下午3:06:14
	 * @param src
	 * @param defaultValue
	 * @return
	 */
	public static String convertEmptyStr(String src, String defaultValue) {
		if (isEmpty(src)) {
			return defaultValue;
		}
		return src;
	}

	/*
	 * public static String getBase64Str(String json) { try { byte[] data = json.getBytes("UTF-8"); data =
	 * GZIPUtils.compress(data); // 压缩 return Base64Ext.encode(data); } catch (Throwable t) { t.printStackTrace(); }
	 * return ""; }
	 * 
	 * public static String getStrFromBase64(String base64Str) { try { byte[] data = Base64Ext.decode(base64Str); data =
	 * GZIPUtils.decompress(data); // 解压 return new String(data, "UTF-8"); } catch (Throwable t) { t.printStackTrace();
	 * } return ""; }
	 */

	/**
	 * 将unixstamp时间戳转换为 去掉时分秒的整天时间戳 线程不安全
	 */
	public static int truncateDate(int unixTimestamp, int defaultValue) {
		calendar.setTimeInMillis(unixTimestamp * 1000L);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		return (int) (calendar.getTimeInMillis() / 1000);
	}

	/**
	 * 将unixstamp时间戳转换为 去掉时分秒的整天时间戳 线程不安全
	 */
	public static int truncateDate(String unixTimestamp, int defaultValue) {
		return truncateDate(StringUtil.convertInt(unixTimestamp, 0), defaultValue);
	}

	/**
	 * <pre>
	 * 将字符串泛型类型的集合以特定的分隔符拼接
	 * @param cllection
	 * @param separator
	 * @return
	 */
	public static String join(Collection<String> collection, String separator) {
		StringBuilder sb = new StringBuilder();
		for (String item : collection) {
			sb.append(item).append(separator);
		}
		// 去掉最后的分隔符
		if (sb.length() == 0) {
			return "";
		} else {
			return sb.substring(0, sb.length() - separator.length());
		}
	}

	public static String join(String[] arr, String separator) {
		StringBuilder sb = new StringBuilder();
		for (String item : arr) {
			sb.append(item).append(separator);
		}
		// 去掉最后的分隔符
		if (sb.length() == 0) {
			return "";
		} else {
			return sb.substring(0, sb.length() - separator.length());
		}
	}

	public static String getFloatString(float value, int scale) {
		BigDecimal bd = new BigDecimal(value + "");
		bd = bd.setScale(scale, BigDecimal.ROUND_HALF_UP);
		return bd.toString();
	}

	public static String[] merge(String[]... arrays) {
		int length = 0;
		for (String[] array : arrays) {
			length += array.length;
		}
		String[] mergeArray = new String[length];
		int i = 0;
		for (String[] array : arrays) {
			for (String item : array) {
				mergeArray[i++] = item;
			}
			length += array.length;
		}
		return mergeArray;
	}

	private static String getDataDatePath(Configuration config) {
		boolean isHourJob = config.getBoolean("is.hour.job", true);
		System.out.println("is.hour.job:" + isHourJob);
		if (isHourJob) {
			long lastHourTime = DCDateUtil.getLastHourDate(config);
			String yyyyMMddHH = DCDateUtil.date2yyyyMMddHH(lastHourTime * 1000);
			return "/" + yyyyMMddHH.replaceAll("-", "/") + "/";
		} else {
			long dataTime = DCDateUtil.getDataDate23Hour(config);
			String yyyyMMddHH = DCDateUtil.date2yyyyMMddHH(dataTime * 1000);
			return "/" + yyyyMMddHH.replaceAll("-", "/") + "/";
		}
	}

	private static String getBasePath(Configuration config) {
		String basePath = config.get("dc.base.dir");
		return isEmpty(basePath) ? PathConstant.DEFAULT_BASE_DIR : basePath;
	}

	public static String getOutPutPath(Configuration config, String businessType, String filePerFix) {
		return getBasePath(config) + businessType + getDataDatePath(config) + filePerFix;
	}

	public static String getOutPutPath(Configuration config, String businessType) {
		return getOutPutPath(config, businessType, businessType);
	}

	public static String mergeRoleIds(String jsonOne, String jsonTwo) {
		Set<String> setOne = DCJsonUtil.jsonToStringSet(jsonOne);
		return mergeRoleIds(setOne, jsonTwo);
	}

	public static String mergeRoleIds(Set<String> roleIdSet, String jsonTwo) {
		if (null == roleIdSet) {
			roleIdSet = new HashSet<String>();
		}

		Set<String> setTwo = DCJsonUtil.jsonToStringSet(jsonTwo);
		if (setTwo != null && !setTwo.isEmpty()) {
			roleIdSet.addAll(setTwo);
		}

		return DCJsonUtil.getGson().toJson(roleIdSet);
	}

	public static String mergeOlDetail(String jsonOne, String jsonTwo) {
		Map<Integer, Integer> mapOne = DCJsonUtil.jsonToIntegerMap(jsonOne);
		return mergeOlDetail(mapOne, jsonTwo);
	}

	public static String mergeOlDetail(Map<Integer, Integer> onlineMap, String jsonTwo) {
		if (null == onlineMap) {
			onlineMap = new HashMap<Integer, Integer>();
		}

		Map<Integer, Integer> mapTwo = DCJsonUtil.jsonToIntegerMap(jsonTwo);
		if (mapTwo != null && !mapTwo.isEmpty()) {
			for (Entry<Integer, Integer> entry : mapTwo.entrySet()) {
				Integer olTime = onlineMap.get(entry.getKey());
				if (olTime == null) {
					onlineMap.put(entry.getKey(), entry.getValue());
				} else {
					onlineMap.put(entry.getKey(), Math.max(entry.getValue(), olTime));
				}
			}
		}
		return DCJsonUtil.getGson().toJson(onlineMap);
	}

	public static String mergePayDetail(String jsonOne, String jsonTwo) {
		Map<Integer, Float> mapOne = DCJsonUtil.jsonToIntFloatMap(jsonOne);
		return mergePayDetail(mapOne, jsonTwo);
	}

	public static String mergePayDetail(Map<Integer, Float> payMap, String jsonTwo) {
		if (null == payMap) {
			payMap = new HashMap<Integer, Float>();
		}

		Map<Integer, Float> mapTwo = DCJsonUtil.jsonToIntFloatMap(jsonTwo);
		if (mapTwo != null && !mapTwo.isEmpty()) {
			payMap.putAll(mapTwo);
		}
		return DCJsonUtil.getGson().toJson(payMap);
	}

	public static void main(String[] args) {
		Map<Integer, Integer> test = new HashMap<Integer, Integer>();
		test.put(1, 2);
		System.out.println(test);
		test.put(1, 3);
		System.out.println(test);
	}

}
