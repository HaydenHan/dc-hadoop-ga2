package com.dataeye.hadoop.domain.common;

public class MRConstants {

	public static final String ALL_GS = "_ALL_GS";

	public static final int INT_TRUE = 1;
	public static final int INT_FALSE = 0;
	public static final int DAY_IN_SECONDS = 24 * 3600;
	public static final int WEEK_IN_SECONDS = 7 * DAY_IN_SECONDS;
	public static final int MONTH_IN_SECONDS = 30 * DAY_IN_SECONDS;
	public static final int NUM_PLACE_HOLDER = 0;
	public static final String STR_PLACE_HOLDER = "-";
	public static final String STR_EMPTY_SET_JSON = "[]";
	public static final String STR_EMPTY_MAP_JSON = "{}";
	public static final String FIELDS_SEPARATOR = "\t";

	public static final int LOST_DAYS_3 = 3;
	public static final int LOST_DAYS_7 = 7;
	public static final int LOST_DAYS_14 = 14;

	public static final int LTV_MAX_DAYS = 30;
	public static final int LTV_MAX_WEEKS = 12;
	public static final int LTV_MAX_MONTH = 6;

	public static final int RETAIN_MAX_DAYS = 30;
	public static final int RETAIN_MAX_WEEKS = 12;
	public static final int RETAIN_MAX_MONTH = 6;

	public static final int RETAIN_ACTION_TIMES = 1;

	/**
	 * <pre>
	 * 输出文件,目录相关常量
	 * @author Hayden<br>
	 * @date 2015年11月7日 下午6:02:54
	 * <br>
	 */
	public class PathConstant {
		public static final String DEFAULT_BASE_DIR = "/data/ga2/mr/";
		/** 角色滚存 */
		public static final String BUSINESS_ROLE_ROLLING = "roleRolling";
		/** 角色日信息 */
		public static final String BUSINESS_ROLE_DAYINFO = "roleInfo";
		/** 角色留存 */
		public static final String BUSINESS_ROLE_RETAIN = "roleRetain";
		/** 角色留存 */
		public static final String BUSINESS_ROLE_WEEK_RETAIN = "roleWeekRetain";
		/** 角色留存 */
		public static final String BUSINESS_ROLE_MONTH_RETAIN = "roleMonthRetain";
		/** 角色回留 */
		public static final String BUSINESS_ROLE_BACK = "roleBack";
		/** 角色ltv */
		public static final String BUSINESS_ROLE_LTV = "roleLTV";
		/** 角色周ltv */
		public static final String BUSINESS_ROLE_WEEK_LTV = "roleWeekLTV";
		/** 角色月ltv */
		public static final String BUSINESS_ROLE_MONTH_LTV = "roleMonthLTV";
		/** 角色流失 */
		public static final String BUSINESS_ROLE_LOST = "roleLost";

		/** 设备激活（临时目录） */
		public static final String BUSINESS_DEVICE_ACTIVE = "deviceActive";
		/** 帐号创建（临时目录） */
		public static final String BUSINESS_ACCOUNT_CREATE = "accountCreate";

		/** 设备激活（结果目录） */
		public static final String BUSINESS_DEVICE_INFO = "deviceInfo";
		/** 帐号滚存（小时） */
		public static final String BUSINESS_ACCOUNT_ROLLING = "accountRolling";
		/** 帐号的基本信息（新增活跃付费） */
		public static final String BUSINESS_ACCOUNT_INFO = "accountInfo";
		/** 帐号天留存 */
		public static final String BUSINESS_ACCOUNT_RETAIN = "accountRetain";
		/** 帐号回流 */
		public static final String BUSINESS_ACCOUNT_BACK = "accountBack";
		/** 帐号流失 */
		public static final String BUSINESS_ACCOUNT_LOST = "accountLost";
		/** 帐号天 LTV */
		public static final String BUSINESS_ACCOUNT_LTV = "accountLTV";
		/** 帐号周留存 */
		public static final String BUSINESS_ACCOUNT_WEEK_RETAIN = "accountWeekRetain";
		/** 帐号月留存 */
		public static final String BUSINESS_ACCOUNT_MONTH_RETAIN = "accountMonthRetain";
		/** 帐号周 LTV */
		public static final String BUSINESS_ACCOUNT_WEEK_LTV = "accountWeekLTV";
		/** 帐号月 LTV */
		public static final String BUSINESS_ACCOUNT_MONTH_LTV = "accountMonthLTV";

		/** 自定义事件 */
		public static final String BUSINESS_EVENT_ATTR = "eventAttr";

	}
}
