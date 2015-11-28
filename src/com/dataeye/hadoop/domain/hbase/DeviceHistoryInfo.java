package com.dataeye.hadoop.domain.hbase;

public class DeviceHistoryInfo {

	private String appId; // appid
	private String platform; // 平台
	private String uid; // 注册时 uid
	private String appVer; // 版本
	private String channel; // 渠道
	private String country; // 国家
	private String province; // 省份
	private String city; // 城市
	private String mac; // mac
	private String imei1; // 手机设备标识
	private String imsi1; // 手机SIM卡标识
	private String imei2;
	private String imsi2;
	private String idfa;
	private String idfv;
	private String model;
	private String brand; // 设备品牌
	private String manu; // 制造商
	private String screen; // 分辨率
	private String cpu;
	private String root; // 是否root
	private String lang; // 语言版本
	private String oper; // 运营商
	private String operISO; // 运营商编号
	private String os; // 操作系统版本
	private String tz; // 注册时时区
	private String ip; // 注册时 ip
	private String net; // 注册时网络
	private String longitude; // 经度
	private String latitude; // 维度
	
	// 激活时间
	private int activeTime;
	// 激活日期
	private int activeDate;
	
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
	public String getPlatform() {
		return platform;
	}
	public void setPlatform(String platform) {
		this.platform = platform;
	}
	public String getChannel() {
		return channel;
	}
	public void setChannel(String channel) {
		this.channel = channel;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getProvince() {
		return province;
	}
	public void setProvince(String province) {
		this.province = province;
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
	public String getManu() {
		return manu;
	}
	public void setManu(String manu) {
		this.manu = manu;
	}
	public String getScreen() {
		return screen;
	}
	public void setScreen(String screen) {
		this.screen = screen;
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
	public String getLang() {
		return lang;
	}
	public void setLang(String lang) {
		this.lang = lang;
	}
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
	public String getLongitude() {
		return longitude;
	}
	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}
	public String getLatitude() {
		return latitude;
	}
	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}
	
	public int getActiveTime() {
		return activeTime;
	}
	
	public void setActiveTime(int activeTime) {
		this.activeTime = activeTime;
	}
	
	public int getActiveDate() {
		return activeDate;
	}
	
	public void setActiveDate(int activeDate) {
		this.activeDate = activeDate;
	}
}
