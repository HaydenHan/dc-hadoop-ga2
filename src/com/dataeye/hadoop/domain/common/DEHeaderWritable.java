package com.dataeye.hadoop.domain.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.RawLog.Event;
import com.dataeye.hadoop.domain.common.RawLog.Header;
import com.dataeye.hadoop.util.StringUtil;

public class DEHeaderWritable extends DEWritable {

	public int fieldsIndex = 0;
	private int timestamp;
	private String appId = MRConstants.STR_PLACE_HOLDER;
	// 版本
	private String appVer = MRConstants.STR_PLACE_HOLDER;
	// 平台
	private String platform = MRConstants.STR_PLACE_HOLDER;
	// 渠道
	private String channel = MRConstants.STR_PLACE_HOLDER;
	// 国家
	private String country = MRConstants.STR_PLACE_HOLDER;
	// 省份
	private String province = MRConstants.STR_PLACE_HOLDER;
	// 城市
	private String city = MRConstants.STR_PLACE_HOLDER;
	// uid
	private String uid = MRConstants.STR_PLACE_HOLDER;
	// mac
	private String mac = MRConstants.STR_PLACE_HOLDER;
	// 手机设备标识
	private String imei1 = MRConstants.STR_PLACE_HOLDER;
	// 手机SIM卡标识
	private String imsi1 = MRConstants.STR_PLACE_HOLDER;
	private String imei2 = MRConstants.STR_PLACE_HOLDER;
	private String imsi2 = MRConstants.STR_PLACE_HOLDER;
	private String idfa = MRConstants.STR_PLACE_HOLDER;
	private String idfv = MRConstants.STR_PLACE_HOLDER;
	private String model = MRConstants.STR_PLACE_HOLDER;
	private String brand = MRConstants.STR_PLACE_HOLDER;
	// 制造商
	private String manu = MRConstants.STR_PLACE_HOLDER;
	// 分辨率
	private String screen = MRConstants.STR_PLACE_HOLDER;
	private String cpu = MRConstants.STR_PLACE_HOLDER;
	// 是否root
	private String root = MRConstants.STR_PLACE_HOLDER;
	// 语言
	private String lang = MRConstants.STR_PLACE_HOLDER;
	// 运营商
	private String oper = MRConstants.STR_PLACE_HOLDER;
	// 运营商编号
	private String operISO = MRConstants.STR_PLACE_HOLDER;
	// 操作系统版本
	private String os = MRConstants.STR_PLACE_HOLDER;
	// 时区
	private String timeZone = MRConstants.STR_PLACE_HOLDER;
	private String ip = MRConstants.STR_PLACE_HOLDER;
	// 上报网络
	private String net = MRConstants.STR_PLACE_HOLDER;
	// 经度
	private String longitude = MRConstants.STR_PLACE_HOLDER;
	// 维度
	private String latitude = MRConstants.STR_PLACE_HOLDER;

	public int getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(int ts) {
		this.timestamp = ts;
	}

	public String getAppId() {
		return appId;
	}

	public int getFieldsIndex() {
		return fieldsIndex;
	}

	public void setFieldsIndex(int fieldsIndex) {
		this.fieldsIndex = fieldsIndex;
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

	public String getTimeZone() {
		return timeZone;
	}

	public void setTimeZone(String timeZone) {
		this.timeZone = timeZone;
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

	public DEHeaderWritable() {
	};

	public DEHeaderWritable(DEHeaderWritable header) {
		this.timestamp = header.getTimestamp();
		this.appId = header.getAppId();
		this.appVer = header.getAppVer();
		this.platform = header.getPlatform();
		this.channel = header.getChannel();
		this.country = header.getCountry();
		this.province = header.getProvince();
		this.city = header.getCity();
		this.uid = header.getUid();
		this.mac = header.getMac();
		this.imei1 = header.getImei1();
		this.imsi1 = header.getImsi1();
		this.imei2 = header.getImei2();
		this.imsi2 = header.getImsi2();
		this.idfa = header.getIdfa();
		this.idfv = header.getIdfv();
		this.model = header.getModel();
		this.brand = header.getBrand();
		this.manu = header.getManu();
		this.screen = header.getScreen();
		this.cpu = header.getCpu();
		this.root = header.getRoot();
		this.lang = header.getLang();
		this.oper = header.getOper();
		this.operISO = header.getOperISO();
		this.os = header.getOs();
		this.timeZone = header.getTimeZone();
		this.ip = header.getIp();
		this.net = header.getNet();
		this.longitude = header.getLongitude();
		this.latitude = header.getLatitude();
	};

	public DEHeaderWritable(String[] fields) {
		this.timestamp = StringUtil.convertInt(fields[fieldsIndex++], 0);
		this.appId = fields[fieldsIndex++];
		this.appVer = fields[fieldsIndex++];
		this.platform = fields[fieldsIndex++];
		this.channel = fields[fieldsIndex++];
		this.country = fields[fieldsIndex++];
		this.province = fields[fieldsIndex++];
		this.city = fields[fieldsIndex++];
		this.uid = fields[fieldsIndex++];
		this.mac = fields[fieldsIndex++];
		this.imei1 = fields[fieldsIndex++];
		this.imsi1 = fields[fieldsIndex++];
		this.imei2 = fields[fieldsIndex++];
		this.imsi2 = fields[fieldsIndex++];
		this.idfa = fields[fieldsIndex++];
		this.idfv = fields[fieldsIndex++];
		this.model = fields[fieldsIndex++];
		this.brand = fields[fieldsIndex++];
		this.manu = fields[fieldsIndex++];
		this.screen = fields[fieldsIndex++];
		this.cpu = fields[fieldsIndex++];
		this.root = fields[fieldsIndex++];
		this.lang = fields[fieldsIndex++];
		this.oper = fields[fieldsIndex++];
		this.operISO = fields[fieldsIndex++];
		this.os = fields[fieldsIndex++];
		this.timeZone = fields[fieldsIndex++];
		this.ip = fields[fieldsIndex++];
		this.net = fields[fieldsIndex++];
		this.longitude = fields[fieldsIndex++];
		this.latitude = fields[fieldsIndex++];
	};

	public DEHeaderWritable(Header header, Event event) {
		this.timestamp = event.getStartTime();
		this.appId = header.getAppId();
		this.appVer = header.getAppVer();
		this.platform = header.getPlat();
		this.channel = header.getCh();
		this.country = header.getCnty();
		this.province = header.getProv();
		this.city = header.getCity();
		this.uid = header.getUid();
		this.mac = header.getMac();
		this.imei1 = header.getImei1();
		this.imsi1 = header.getImsi1();
		this.imei2 = header.getImei2();
		this.imsi2 = header.getImsi2();
		this.idfa = header.getIdfa();
		this.idfv = header.getIdfv();
		this.model = header.getModel();
		this.brand = header.getBrand();
		this.manu = header.getManu();
		this.screen = header.getScreen();
		this.cpu = header.getCpu();
		this.root = header.getRoot();
		this.lang = header.getLang();
		this.oper = header.getOper();
		this.operISO = header.getOper();
		this.os = header.getOs();
		this.timeZone = header.getTz();
		this.ip = header.getIp();
		this.net = header.getNet();
		this.longitude = header.getLon();
		this.latitude = header.getLat();
	}

	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.timestamp);
		out.writeUTF(this.appId);
		out.writeUTF(this.appVer);
		out.writeUTF(this.platform);
		out.writeUTF(this.channel);
		out.writeUTF(this.country);
		out.writeUTF(this.province);
		out.writeUTF(this.city);
		out.writeUTF(this.uid);
		out.writeUTF(this.mac);
		out.writeUTF(this.imei1);
		out.writeUTF(this.imsi1);
		out.writeUTF(this.imei2);
		out.writeUTF(this.imsi2);
		out.writeUTF(this.idfa);
		out.writeUTF(this.idfv);
		out.writeUTF(this.model);
		out.writeUTF(this.brand);
		out.writeUTF(this.manu);
		out.writeUTF(this.screen);
		out.writeUTF(this.cpu);
		out.writeUTF(this.root);
		out.writeUTF(this.lang);
		out.writeUTF(this.oper);
		out.writeUTF(this.operISO);
		out.writeUTF(this.os);
		out.writeUTF(this.timeZone);
		out.writeUTF(this.ip);
		out.writeUTF(this.net);
		out.writeUTF(this.longitude);
		out.writeUTF(this.latitude);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.timestamp = in.readInt();
		this.appId = in.readUTF();
		this.appVer = in.readUTF();
		this.platform = in.readUTF();
		this.channel = in.readUTF();
		this.country = in.readUTF();
		this.province = in.readUTF();
		this.city = in.readUTF();
		this.uid = in.readUTF();
		this.mac = in.readUTF();
		this.imei1 = in.readUTF();
		this.imsi1 = in.readUTF();
		this.imei2 = in.readUTF();
		this.imsi2 = in.readUTF();
		this.idfa = in.readUTF();
		this.idfv = in.readUTF();
		this.model = in.readUTF();
		this.brand = in.readUTF();
		this.manu = in.readUTF();
		this.screen = in.readUTF();
		this.cpu = in.readUTF();
		this.root = in.readUTF();
		this.lang = in.readUTF();
		this.oper = in.readUTF();
		this.operISO = in.readUTF();
		this.os = in.readUTF();
		this.timeZone = in.readUTF();
		this.ip = in.readUTF();
		this.net = in.readUTF();
		this.longitude = in.readUTF();
		this.latitude = in.readUTF();
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeUTF8WithSep(out, this.timestamp);
		super.writeUTF8WithSep(out, this.appId);
		super.writeUTF8WithSep(out, this.appVer);
		super.writeUTF8WithSep(out, this.platform);
		super.writeUTF8WithSep(out, this.channel);
		super.writeUTF8WithSep(out, this.country);
		super.writeUTF8WithSep(out, this.province);
		super.writeUTF8WithSep(out, this.city);
		super.writeUTF8WithSep(out, this.uid);
		super.writeUTF8WithSep(out, this.mac);
		super.writeUTF8WithSep(out, this.imei1);
		super.writeUTF8WithSep(out, this.imsi1);
		super.writeUTF8WithSep(out, this.imei2);
		super.writeUTF8WithSep(out, this.imsi2);
		super.writeUTF8WithSep(out, this.idfa);
		super.writeUTF8WithSep(out, this.idfv);
		super.writeUTF8WithSep(out, this.model);
		super.writeUTF8WithSep(out, this.brand);
		super.writeUTF8WithSep(out, this.manu);
		super.writeUTF8WithSep(out, this.screen);
		super.writeUTF8WithSep(out, this.cpu);
		super.writeUTF8WithSep(out, this.root);
		super.writeUTF8WithSep(out, this.lang);
		super.writeUTF8WithSep(out, this.oper);
		super.writeUTF8WithSep(out, this.operISO);
		super.writeUTF8WithSep(out, this.os);
		super.writeUTF8WithSep(out, this.timeZone);
		super.writeUTF8WithSep(out, this.ip);
		super.writeUTF8WithSep(out, this.net);
		super.writeUTF8WithSep(out, this.longitude);
		super.writeUTF8(out, this.latitude);// the final one don't need a separator
	}

}
