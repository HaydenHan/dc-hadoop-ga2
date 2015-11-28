package com.dataeye.hadoop.domain.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.dataeye.hadoop.common.dewritable.DEWritable;
import com.dataeye.hadoop.domain.common.MRConstants;
import com.dataeye.hadoop.domain.common.RawLog.Event;
import com.dataeye.hadoop.domain.common.RawLog.Header;
import com.dataeye.hadoop.util.DCJsonUtil;

public class MKEventSelf extends DEWritable {
	private int timestamp;
	private String appId = MRConstants.STR_PLACE_HOLDER;
	private String appVer = MRConstants.STR_PLACE_HOLDER;
	private String platform = MRConstants.STR_PLACE_HOLDER;
	private String channel = MRConstants.STR_PLACE_HOLDER;
	private String gameserver = MRConstants.STR_PLACE_HOLDER;
	private String country = MRConstants.STR_PLACE_HOLDER;
	private String province = MRConstants.STR_PLACE_HOLDER;
	private String city = MRConstants.STR_PLACE_HOLDER;
	private String accountId = MRConstants.STR_PLACE_HOLDER;
	private String uid = MRConstants.STR_PLACE_HOLDER;
	private String eventId = MRConstants.STR_PLACE_HOLDER;
	private int duration;
	private String eventAttrMap = MRConstants.STR_EMPTY_MAP_JSON;

	public int getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(int timestamp) {
		this.timestamp = timestamp;
	}

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

	public String getGameserver() {
		return gameserver;
	}

	public void setGameserver(String gameserver) {
		this.gameserver = gameserver;
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

	public String getAccountId() {
		return accountId;
	}

	public void setAccountId(String accountId) {
		this.accountId = accountId;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getEventId() {
		return eventId;
	}

	public void setEventId(String eventId) {
		this.eventId = eventId;
	}

	public int getDuration() {
		return duration;
	}

	public void setDuration(int duration) {
		this.duration = duration;
	}

	public String getEventAttrMap() {
		return eventAttrMap;
	}

	public void setEventAttrMap(String eventAttrMap) {
		this.eventAttrMap = eventAttrMap;
	}

	public MKEventSelf() {

	}

	public MKEventSelf(Header header, Event event) {
		this.timestamp = event.getStartTime();
		this.appId = header.getAppId();
		this.appVer = header.getAppVer();
		this.platform = header.getPlat();
		this.channel = header.getCh();
		this.gameserver = event.getGameServer();
		this.country = header.getCnty();
		this.province = header.getProv();
		this.city = header.getCity();
		this.accountId = event.getAccountId();
		this.uid = header.getUid();
		this.eventId = event.getEventId();
		this.duration = event.getEndTime() - event.getStartTime();
		this.eventAttrMap = DCJsonUtil.getGson().toJson(event.getLabelMap());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.timestamp = in.readInt();
		this.appId = in.readUTF();
		this.appVer = in.readUTF();
		this.platform = in.readUTF();
		this.channel = in.readUTF();
		this.gameserver = in.readUTF();
		this.country = in.readUTF();
		this.province = in.readUTF();
		this.city = in.readUTF();
		this.accountId = in.readUTF();
		this.uid = in.readUTF();
		this.eventId = in.readUTF();
		this.duration = in.readInt();
		this.eventAttrMap = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.timestamp);
		out.writeUTF(this.appId);
		out.writeUTF(this.appVer);
		out.writeUTF(this.platform);
		out.writeUTF(this.channel);
		out.writeUTF(this.gameserver);
		out.writeUTF(this.country);
		out.writeUTF(this.province);
		out.writeUTF(this.city);
		out.writeUTF(this.accountId);
		out.writeUTF(this.uid);
		out.writeUTF(this.eventId);
		out.writeInt(this.duration);
		out.writeUTF(this.eventAttrMap);
	}

	@Override
	public int compareTo(DEWritable o) {
		return CompareToBuilder.reflectionCompare(this, o);
	}

	@Override
	public void writeAsString(DataOutput out) throws IOException {
		super.writeUTF8WithSep(out, this.timestamp);
		super.writeUTF8WithSep(out, this.appId);
		super.writeUTF8WithSep(out, this.appVer);
		super.writeUTF8WithSep(out, this.platform);
		super.writeUTF8WithSep(out, this.channel);
		super.writeUTF8WithSep(out, this.gameserver);
		super.writeUTF8WithSep(out, this.country);
		super.writeUTF8WithSep(out, this.province);
		super.writeUTF8WithSep(out, this.city);
		super.writeUTF8WithSep(out, this.accountId);
		super.writeUTF8WithSep(out, this.uid);
		super.writeUTF8WithSep(out, this.eventId);
		super.writeUTF8WithSep(out, this.duration);
		super.writeUTF8(out, this.eventAttrMap);
	}

}
