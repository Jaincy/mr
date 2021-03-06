package com.yp114.omc.apponofflog.retained;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class NspAOOLogDayRegionRetainProcessor_2 extends Configured implements Tool{
	//2，计算出 这两天连续登陆的用户数
	
	public static void main(String[] args) {
		try {
			ToolRunner.run(new NspAOOLogDayRegionRetainProcessor_2(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "NspAOOLogDayRegionRetainProcessor_2");
		job.setJarByClass(NspAOOLogDayRegionRetainProcessor_2.class);
		job.setMapperClass(AOODayRegionMapper_2.class);
		job.setReducerClass(AOODayRegionReducer_2.class);
		
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
			 job.setMapOutputKeyClass(Text.class);
		     job.setMapOutputValueClass(IntWritable.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class);
//		  job.setOutputValueClass(IntWritable.class);
		  
		  return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class AOODayRegionMapper_2 extends Mapper<Object, Text, Text, IntWritable>{
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			String string = value.toString();
			String[] split = string.split("\t");
			String line = split[0];
			int count = Integer.parseInt(split[1]);
			//XXXXXX#t#Z#i#a1000023180b42	2
			if(count>1){
				line=line.substring(0, line.indexOf("#i#"));	
				context.write(new Text(line), new IntWritable(1));
			}
		
		}
		
	}
	
	public static class AOODayRegionReducer_2 extends Reducer<Text, IntWritable, Text, Text>{
		
		private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			//result.set(sum);
			context.write(key, new Text(sum+"A"));
		}
	}
	

}
