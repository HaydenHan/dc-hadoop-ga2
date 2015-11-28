package com.dataeye.hadoop.mapreduce;


public class RollingDataWrapDriver {
	//
	// /**
	// * @param args
	// * @throws IOException
	// * @throws Exception
	// * @throws
	// */
	// public static void main(String[] args) throws Exception {
	// int i = 0;
	// int redNum = Integer.valueOf(args[i++]);
	// String inputPath1 = args[i++];
	// String inputPath2 = args[i++];
	// String outputPath = args[i++];
	//
	// Configuration conf = new Configuration();
	// Job job = new Job(conf, "RollingDataWrap");
	// job.setJarByClass(RollingDataWrapMaper.class);
	// //map
	// job.setMapperClass(RollingDataWrapMaper.class);
	// job.setMapOutputKeyClass(OutFieldsBaseModel.class);
	// job.setMapOutputValueClass(OutFieldsBaseModel.class);
	// //reduce
	// job.setReducerClass(RollingDataWrapReducer.class);
	// job.setOutputKeyClass(OutFieldsBaseModel.class);
	// job.setOutputValueClass(NullWritable.class);
	// job.setNumReduceTasks(redNum);
	// FileInputFormat.addInputPath(job, new Path(inputPath1));
	// FileInputFormat.addInputPath(job, new Path(inputPath2));
	// FileOutputFormat.setOutputPath(job, new Path(outputPath));
	//
	// System.exit((job.waitForCompletion(true)) ? 0 : 1);
	// }
}
