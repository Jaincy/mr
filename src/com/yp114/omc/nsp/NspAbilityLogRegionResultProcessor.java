package com.yp114.omc.nsp;

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

import com.yp114.omc.test.text;

public class NspAbilityLogRegionResultProcessor extends Configured implements Tool{
	/*
	 * FATALLOG IMEI日分省统计数据
	 * 统计结果
	 * */
	public static void main(String[] args) {
		try {
			ToolRunner.run(new NspAbilityLogRegionResultProcessor(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		 if (args.length != 2){

	           System.err.printf("Usage: %s <input><output>",getClass().getSimpleName());

	           ToolRunner.printGenericCommandUsage(System.err);

	           return -1;                  
	          }
		 Configuration conf =getConf();
		 Job job =Job.getInstance(conf, "NspAbilityLogRegionResultProcessor");
		 job.setJarByClass(NspAbilityLogRegionResultProcessor.class);
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  
		  job.setMapperClass(NspLogRegionResultMapper.class);
		  job.setCombinerClass(NspLogRegionResultReducer.class);
		  job.setReducerClass(NspLogRegionResultReducer.class);
		  
		  
		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(IntWritable.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(IntWritable.class);
		 
		 return job.waitForCompletion(true)?0:1; 
	}
	
	
	public static class NspLogRegionResultMapper extends Mapper<Object, Text, Text, IntWritable>{
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
				String string = value.toString();
				String[] split = string.split("\t");
				String line = split[0];
				line = line.substring(0,line.indexOf("#i#"));
				context.write(new Text(line), new IntWritable(1));
		
		}
		
	}
	public static class NspLogRegionResultReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum=0;
			for (IntWritable i : values) {
				sum+=i.get();
			}
			context.write(key, new IntWritable(sum));
		
		}
		
	}
	

}
