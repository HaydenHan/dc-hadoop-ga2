//package com.dataeye.hadoop.mapreduce;
//
//import java.io.IOException;
//
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
//
//public class RollingDataWrapReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable>{
//
//	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
//	private MultipleOutputs<OutFieldsBaseModel, NullWritable> mos;
//	
//	@Override
//	protected void setup(Context context) throws IOException, InterruptedException {
//		mos = new MultipleOutputs<OutFieldsBaseModel, NullWritable>(context);
//	}
//	
//	@Override
//	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
//		
//		String maxVersion = "";
//		String base64Info = null;
//		String extInfo = null;
//		for (OutFieldsBaseModel val : values) {
//			int i = 0;
//			String flag = val.getOutFields()[i++];
//			String version = val.getOutFields()[i++];
//			String info = val.getOutFields()[i++];
//			
//			if(version.compareTo(maxVersion) > 0){
//				maxVersion = version; 
//			}
//			
//			if(RollingDataWrapMaper.FLAG_USER_INFO.equals(flag)){
//				base64Info = info;
//			}else if(RollingDataWrapMaper.FLAG_EXT_INFO.equals(flag)){
//				extInfo = info;
//			}
//		}
//		
//		if(null == base64Info){//应该不可能出现这种
//			return;
//		}
//		
//		String appId = key.getOutFields()[0];
//		String platform = key.getOutFields()[1];
//		String gameServer = key.getOutFields()[2];
//		String accountId = key.getOutFields()[3];
//		
//		String prefix = appId + "_" + platform;
//		if(prefix.contains(":")){
//			return;
//		}
//		String[] keyFields = new String[]{
//				appId + "|" + maxVersion,
//				platform,
//				gameServer,
//				accountId,
//				base64Info,
//				extInfo == null ? "-" : extInfo
//		};
//		keyObj.setOutFields(keyFields);
//		
//		/*keyObj.set(appId+"|"+maxVersion + MRConstants.SEPERATOR_IN
//					+ platform + MRConstants.SEPERATOR_IN
//					+ gameServer + MRConstants.SEPERATOR_IN
//					+ accountId + MRConstants.SEPERATOR_IN
//					+ base64Info + MRConstants.SEPERATOR_IN
//					+ extInfo);*/
//		//输出
//		//context.write(keyObj, NullWritable.get());
//		mos.write(keyObj, NullWritable.get(), prefix);
//		
//	}
//	
//	@Override
//	protected void cleanup(Context context) throws IOException, InterruptedException {
//		mos.close();
//	}
// }
