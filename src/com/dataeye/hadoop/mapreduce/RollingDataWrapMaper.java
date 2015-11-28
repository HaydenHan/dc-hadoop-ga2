//package com.dataeye.hadoop.mapreduce;
//
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//
///**
// * 
// * 以  UserInfoRollingDay 和  UserExtInfoRollingDay 作为输入
// * 
// * 以 appId, platform, gameServer, accountId 为 key, 把两者信息揉到一起 
// * 
// * 输出：
// * appId, platform, gameServer, accountId, base64Info, extInfo
// */
//public class RollingDataWrapMaper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel>{
//
//	// static final String FLAG_USER_INFO = "uInfo";
//	// static final String FLAG_EXT_INFO = "eInfo";
//	//
//	// private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
//	// private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
//	// // 当前输入的文件后缀
//	// private String fileSuffix = "";
//	// private Date scheduleTime = null;
//	// private int statDate = 0;
//	// // 最后一次登录是6个月（半年之前）的玩家剔除掉
//	// private int keepPlayerTime = 6 * 30 * 24 * 3600;
//	//
//	// @Override
//	// protected void setup(Context context) throws IOException, InterruptedException {
//	// fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
//	// Calendar cal = Calendar.getInstance();
//	// scheduleTime = cal.getTime();
//	// statDate = (int)(cal.getTimeInMillis()/1000);
//	// }
//	//
//	// @Override
//	// protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//	// String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
//	//
//	// if(fileSuffix.contains(Constants.SUFFIX_USERROLLING)){
//	// UserInfoRollingLog userInfoRollingLog = new UserInfoRollingLog(scheduleTime, paraArr);
//	// String[] appIdAndVersion = userInfoRollingLog.getAppID().split("\\|");
//	// if(appIdAndVersion.length < 2){
//	// return;
//	// }
//	//
//	// String pureAppId = appIdAndVersion[0];
//	// String AppVersion = appIdAndVersion[1];
//	// String platform = userInfoRollingLog.getPlatform();
//	// String accountId = userInfoRollingLog.getAccountID();
//	// String gameServer = userInfoRollingLog.getPlayerDayInfo().getGameRegion();
//	// String base64Info = userInfoRollingLog.getInfoBase64();
//	//
//	// //appId 长度必须为 32
//	// if(32 != pureAppId.length() || pureAppId.contains(":")){
//	// return;
//	// }
//	//
//	// // 最后一次登录是6个月（半年之前）的玩家剔除掉
//	// if((statDate - userInfoRollingLog.getPlayerDayInfo().getLastLoginDate()) > keepPlayerTime){
//	// return;
//	// }
//	//
//	// String[] keyFields = new String[]{
//	// pureAppId,
//	// platform,
//	// gameServer,
//	// accountId
//	// };
//	// String[] valFields = new String[]{
//	// FLAG_USER_INFO,
//	// AppVersion,
//	// base64Info
//	// };
//	// keyObj.setOutFields(keyFields);
//	// valObj.setOutFields(valFields);
//	// context.write(keyObj, valObj);
//	//
//	// }else if(fileSuffix.contains(Constants.SUFFIX_EXT_INFO_ROLL_DAY)){
//	// UserExtInfoRollingLog extInfoRollingLog = new UserExtInfoRollingLog(paraArr);
//	//
//	// String[] appInfo = extInfoRollingLog.getAppId().split("\\|");
//	// if(appInfo.length < 2){
//	// return;
//	// }
//	// //appId 长度必须为 32
//	// if(32 != appInfo[0].length() || appInfo[0].contains(":")){
//	// return;
//	// }
//	// String[] keyFields = new String[]{
//	// appInfo[0], // appId without version
//	// extInfoRollingLog.getPlatform(),
//	// extInfoRollingLog.getGameServer(),
//	// extInfoRollingLog.getAccountID()
//	// };
//	//
//	// String[] valFields = new String[]{
//	// FLAG_EXT_INFO,
//	// appInfo[1], // appVersion
//	// extInfoRollingLog.getDetailInfo()
//	// };
//	//
//	// keyObj.setOutFields(keyFields);
//	// valObj.setOutFields(valFields);
//	// context.write(keyObj, valObj);
//	// }
//	// }
// }
