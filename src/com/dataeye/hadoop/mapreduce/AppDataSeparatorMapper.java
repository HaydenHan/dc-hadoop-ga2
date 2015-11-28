package com.dataeye.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class AppDataSeparatorMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private MultipleOutputs<Text, NullWritable> mos;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<Text, NullWritable>(context);
	}

	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		String[] array = value.toString().split("\t");
		
		String[] appIdAndVersion = array[0].split("\\|");
		if(appIdAndVersion.length < 2){
			return;
		}
		
		String pureAppId = appIdAndVersion[0];
		String platform = array[1];
		String prefix = pureAppId + "_" + platform;
		mos.write(value, NullWritable.get(), prefix);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}

	public static void main(String[] args) throws Exception {
		/*String jarFile = args[0];
		String inputPath = args[1];
		String outputPath = args[2];
		
		Configuration conf = new Configuration();
		conf.set("mapred.jar", jarFile);
		conf.set("mapreduce.map.class", "com.dataeye.hadoop.mapreduce.AppDataSeparatorMapper");
		conf.set("mapred.mapoutput.key.class", "org.apache.hadoop.io.Text");
		conf.set("mapred.mapoutput.value.class", "org.apache.hadoop.io.NullWritable");
		conf.set("mapred.reduce.tasks", "0");
		conf.set("mapreduce.inputformat.class", "org.apache.hadoop.mapreduce.lib.input.TextInputFormat");
		conf.set("mapred.job.name", "AppDataSeparator");
		conf.set("mapred.input.dir", inputPath);
		conf.set("mapred.output.dir", outputPath);
		Job job = new Job(conf);*/
		
		String inputPath = args[0];
		String outputPath = args[1];
		
		Configuration conf = new Configuration();
	    Job job = new Job(conf, "AppSep");
	    job.setJarByClass(AppDataSeparatorMapper.class);
	    job.setMapperClass(AppDataSeparatorMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(NullWritable.class);
	    job.setNumReduceTasks(0);
	    FileInputFormat.addInputPath(job, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		System.exit((job.waitForCompletion(true)) ? 0 : 1);
	}
}
