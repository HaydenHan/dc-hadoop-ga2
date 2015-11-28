package com.dataeye.hadoop.domain.common;

import java.util.Map;

import com.dataeye.hadoop.util.StringUtil;

public class RawLog {

	private Header header;
	private Event[] event;

	public Header getHeader() {
		return header;
	}

	public void setHeader(Header header) {
		this.header = header;
	}

	public Event[] getEvent() {
		return event;
	}

	public void setEvent(Event[] event) {
		this.event = event;
	}

	public static class Header {
		public String appId = MRConstants.STR_PLACE_HOLDER;
		// 版本
		public String appVer = MRConstants.STR_PLACE_HOLDER;
		// 平台
		public String plat = MRConstants.STR_PLACE_HOLDER;
		// 渠道
		public String ch = MRConstants.STR_PLACE_HOLDER;
		// 国家
		public String cnty = MRConstants.STR_PLACE_HOLDER;
		// 省份
		public String prov = MRConstants.STR_PLACE_HOLDER;
		// 城市
		public String city = MRConstants.STR_PLACE_HOLDER;
		// uid
		public String uid = MRConstants.STR_PLACE_HOLDER;
		// mac
		public String mac = MRConstants.STR_PLACE_HOLDER;
		// 手机设备标识
		public String imei1 = MRConstants.STR_PLACE_HOLDER;
		// 手机SIM卡标识
		public String imsi1 = MRConstants.STR_PLACE_HOLDER;
		public String imei2 = MRConstants.STR_PLACE_HOLDER;
		public String imsi2 = MRConstants.STR_PLACE_HOLDER;
		public String idfa = MRConstants.STR_PLACE_HOLDER;
		public String idfv = MRConstants.STR_PLACE_HOLDER;
		public String model = MRConstants.STR_PLACE_HOLDER;
		public String brand = MRConstants.STR_PLACE_HOLDER;
		// 制造商
		public String manu = MRConstants.STR_PLACE_HOLDER;
		// 分辨率
		public String screen = MRConstants.STR_PLACE_HOLDER;
		// public String resolution = MRConstants.STR_PLACE_HOLDER;
		public String cpu = MRConstants.STR_PLACE_HOLDER;
		// 是否root
		public String root = MRConstants.STR_PLACE_HOLDER;
		// 语言
		public String lang = MRConstants.STR_PLACE_HOLDER;
		// public String language = MRConstants.STR_PLACE_HOLDER;
		// 运营商
		public String oper = MRConstants.STR_PLACE_HOLDER;
		// 运营商编号
		public String operISO = MRConstants.STR_PLACE_HOLDER;
		// 操作系统版本
		public String os = MRConstants.STR_PLACE_HOLDER;
		// public String osVersion = MRConstants.STR_PLACE_HOLDER;
		// 时区
		public String tz = MRConstants.STR_PLACE_HOLDER;
		public String ip = MRConstants.STR_PLACE_HOLDER;
		// 上报网络
		public String net = MRConstants.STR_PLACE_HOLDER;
		// 经度
		public String lon = MRConstants.STR_PLACE_HOLDER;
		// 维度
		public String lat = MRConstants.STR_PLACE_HOLDER;

		public String getAppId() {
			return appId;
		}

		public void setAppId(String appId) {
			this.appId = appId;
		}

		public String getAppVer() {
			return appVer;
		}

		public void setAppVer(String appVer) {
			this.appVer = appVer;
		}

		public String getManu() {
			return manu;
		}

		public void setManu(String manu) {
			this.manu = manu;
		}

		public String getCpu() {
			return cpu;
		}

		public void setCpu(String cpu) {
			this.cpu = cpu;
		}

		public String getRoot() {
			return root;
		}

		public void setRoot(String root) {
			this.root = root;
		}

		public String getPlat() {
			return plat;
		}

		public void setPlat(String plat) {
			this.plat = plat;
		}

		public String getCh() {
			return ch;
		}

		public void setCh(String ch) {
			this.ch = ch;
		}

		public String getCnty() {
			return cnty;
		}

		public void setCnty(String cnty) {
			this.cnty = cnty;
		}

		public String getProv() {
			return prov;
		}

		public void setProv(String prov) {
			this.prov = prov;
		}

		public String getCity() {
			return city;
		}

		public void setCity(String city) {
			this.city = city;
		}

		public String getUid() {
			return uid;
		}

		public void setUid(String uid) {
			this.uid = uid;
		}

		public String getMac() {
			return mac;
		}

		public void setMac(String mac) {
			this.mac = mac;
		}

		public String getImei1() {
			return imei1;
		}

		public void setImei1(String imei1) {
			this.imei1 = imei1;
		}

		public String getImsi1() {
			return imsi1;
		}

		public void setImsi1(String imsi1) {
			this.imsi1 = imsi1;
		}

		public String getImei2() {
			return imei2;
		}

		public void setImei2(String imei2) {
			this.imei2 = imei2;
		}

		public String getImsi2() {
			return imsi2;
		}

		public void setImsi2(String imsi2) {
			this.imsi2 = imsi2;
		}

		public String getIdfa() {
			return idfa;
		}

		public void setIdfa(String idfa) {
			this.idfa = idfa;
		}

		public String getIdfv() {
			return idfv;
		}

		public void setIdfv(String idfv) {
			this.idfv = idfv;
		}

		public String getModel() {
			return model;
		}

		public void setModel(String model) {
			this.model = model;
		}

		public String getBrand() {
			return brand;
		}

		public void setBrand(String brand) {
			this.brand = brand;
		}

		public String getScreen() {
			return screen;
		}

		public void setScreen(String screen) {
			this.screen = screen;
		}

		public String getLang() {
			return lang;
		}

		public void setLang(String lang) {
			this.lang = lang;
		}

		// public String getResolution() {
		// return resolution;
		// }
		//
		// public void setResolution(String resolution) {
		// this.resolution = resolution;
		// }
		//
		// public String getLanguage() {
		// return language;
		// }
		//
		// public void setLanguage(String language) {
		// this.language = language;
		// }

		// public String getOsVersion() {
		// return osVersion;
		// }
		//
		// public void setOsVersion(String osVersion) {
		// this.osVersion = osVersion;
		// }

		public String getOper() {
			return oper;
		}

		public void setOper(String oper) {
			this.oper = oper;
		}

		public String getOperISO() {
			return operISO;
		}

		public void setOperISO(String operISO) {
			this.operISO = operISO;
		}

		public String getOs() {
			return os;
		}

		public void setOs(String os) {
			this.os = os;
		}

		public String getTz() {
			return tz;
		}

		public void setTz(String tz) {
			this.tz = tz;
		}

		public String getIp() {
			return ip;
		}

		public void setIp(String ip) {
			this.ip = ip;
		}

		public String getNet() {
			return net;
		}

		public void setNet(String net) {
			this.net = net;
		}

		public String getLon() {
			return lon;
		}

		public void setLon(String lon) {
			this.lon = lon;
		}

		public String getLat() {
			return lat;
		}

		public void setLat(String lat) {
			this.lat = lat;
		}

	}

	public static class Event {

		private String eventId;
		private Map<String, String> labelMap;
		private int startTime;
		private int endTime;

		public String getEventId() {
			return eventId;
		}

		public void setEventId(String eventId) {
			this.eventId = eventId;
		}

		public Map<String, String> getLabelMap() {
			return labelMap;
		}

		public void setLabelMap(Map<String, String> labelMap) {
			this.labelMap = labelMap;
		}

		public int getStartTime() {
			return startTime;
		}

		public void setStartTime(int startTime) {
			this.startTime = startTime;
		}

		public int getEndTime() {
			return endTime;
		}

		public void setEndTime(int endTime) {
			this.endTime = endTime;
		}

		// ---
		public String getGameServer() {
			String gameServer = this.getLabelMap().get(DEEventIds.DES_Gamesvr);
			if (StringUtil.isEmpty(gameServer)) {
				return MRConstants.STR_PLACE_HOLDER;
			}
			return gameServer;
		}

		public String getAccountId() {
			String accountId = this.getLabelMap().get(DEEventIds.DES_AccountId);
			if (StringUtil.isEmpty(accountId)) {
				return MRConstants.STR_PLACE_HOLDER;
			}
			return accountId;
		}

		public String getAccountType() {
			String accountType = this.getLabelMap().get(DEEventIds.KEY_Account_Type);
			if (StringUtil.isEmpty(accountType)) {
				return MRConstants.STR_PLACE_HOLDER;
			}
			return accountType;
		}

		public String getAccountGender() {
			String accountGender = this.getLabelMap().get(DEEventIds.KEY_Account_Gender);
			if (StringUtil.isEmpty(accountGender)) {
				return MRConstants.STR_PLACE_HOLDER;
			}
			return accountGender;
		}

		public String getAccountAge() {
			String accountAge = this.getLabelMap().get(DEEventIds.KEY_Account_Age);
			if (StringUtil.isEmpty(accountAge)) {
				return MRConstants.STR_PLACE_HOLDER;
			}
			return accountAge;
		}

		/*
		 * public int getActiveTime(){ String activeTime = this.getLabelMap().get(DEEventIds.KEY_ACTIVE_TIME);
		 * if(StringUtil.isEmpty(activeTime)){ return MRConstants.NUM_PLACE_HOLDER; } return
		 * StringUtil.convertInt(activeTime, 0); }
		 */

		public int getLoginTime() {
			String loginTime = this.getLabelMap().get(DEEventIds.DES_LoginTime);
			if (StringUtil.isEmpty(loginTime)) {
				return MRConstants.NUM_PLACE_HOLDER;
			}
			return StringUtil.convertInt(loginTime, 0);
		}

		// public int getTrackTime() {
		// String loginTime = this.getLabelMap().get(DEEventIds.KEY_TRACK_TIME);
		// if (StringUtil.isEmpty(loginTime)) {
		// return MRConstants.NUM_PLACE_HOLDER;
		// }
		// return StringUtil.convertInt(loginTime, 0);
		// }

		public int getDesLoginTime() {
			String loginTime = this.getLabelMap().get(DEEventIds.DES_LoginTime);
			if (StringUtil.isEmpty(loginTime)) {
				return MRConstants.NUM_PLACE_HOLDER;
			}
			return StringUtil.convertInt(loginTime, 0);
		}

		public int getOnlineTime() {
			String onlineTime = this.getLabelMap().get(DEEventIds.KEY_ONLINE_TIME);
			if (StringUtil.isEmpty(onlineTime)) {
				return MRConstants.NUM_PLACE_HOLDER;
			}
			return StringUtil.convertInt(onlineTime, 0);
		}

		public int getPayTime() {
			String payTime = this.getLabelMap().get(DEEventIds.KEY_PAY_TIME);
			if (StringUtil.isEmpty(payTime)) {
				return MRConstants.NUM_PLACE_HOLDER;
			}
			return StringUtil.convertInt(payTime, 0);
		}

		public float getPayAmount() {
			String payAmount = this.getLabelMap().get(DEEventIds.KEY_PAY_AMOUNT);
			if (StringUtil.isEmpty(payAmount)) {
				return MRConstants.NUM_PLACE_HOLDER;
			}
			return StringUtil.convertFloat(payAmount, 0);
		}

		public String getPayType() {
			String payType = this.getLabelMap().get(DEEventIds.KEY_PAY_TYPE);
			if (StringUtil.isEmpty(payType)) {
				return MRConstants.STR_PLACE_HOLDER;
			}
			return payType;
		}

		/**
		 * <pre>
		 * 获取角色ID
		 * @return
		 * @author Hayden<br>
		 * @date 2015年11月5日 下午7:37:03
		 * <br>
		 */
		public String getRoleId() {
			String roleId = this.getLabelMap().get(DEEventIds.DES_ROLE_ID);
			if (StringUtil.isEmpty(roleId)) {
				return MRConstants.STR_PLACE_HOLDER;
			}
			return roleId;
		}

		/**
		 * <pre>
		 * 获取角色名称
		 * @return
		 * @author Hayden<br>
		 * @date 2015年11月5日 下午7:37:23
		 * <br>
		 */
		public String getRoleName() {
			String roleName = this.getLabelMap().get(DEEventIds.KEY_ROLE_NAME);
			if (StringUtil.isEmpty(roleName)) {
				return MRConstants.STR_PLACE_HOLDER;
			}
			return roleName;
		}

		/**
		 * 获取角色等级
		 * 
		 * <pre>
		 * @return
		 * @author Hayden<br>
		 * @date 2015年11月5日 下午7:38:54
		 * <br>
		 */
		public String getRoleLevel() {
			String roleLevel = this.getLabelMap().get(DEEventIds.DES_ROLE_LEVEL);
			if (StringUtil.isEmpty(roleLevel)) {
				return MRConstants.STR_PLACE_HOLDER;
			}
			return roleLevel;
		}

		/**
		 * <pre>
		 * 获取角色种族
		 * @return
		 * @author Hayden<br>
		 * @date 2015年11月5日 下午7:40:04
		 * <br>
		 */
		public String getRoleRace() {
			String roleRace = this.getLabelMap().get(DEEventIds.KEY_ROLE_RACE);
			if (StringUtil.isEmpty(roleRace)) {
				return MRConstants.STR_PLACE_HOLDER;
			}
			return roleRace;
		}

		/**
		 * <pre>
		 * 获取角色职业
		 * @return
		 * @author Hayden<br>
		 * @date 2015年11月5日 下午7:40:04
		 * <br>
		 */
		public String getRoleClass() {
			String roleClass = this.getLabelMap().get(DEEventIds.KEY_ROLE_CAREER);
			if (StringUtil.isEmpty(roleClass)) {
				return MRConstants.STR_PLACE_HOLDER;
			}
			return roleClass;
		}

		/**
		 * <pre>
		 * 判断事件属性中是否有角色ID属性
		 * @return
		 * @author Hayden<br>
		 * @date 2015年11月5日 下午7:41:40
		 * <br>
		 */
		public boolean existRoleId() {
			return this.getLabelMap().containsKey(DEEventIds.DES_ROLE_ID);
		}
	}
}
