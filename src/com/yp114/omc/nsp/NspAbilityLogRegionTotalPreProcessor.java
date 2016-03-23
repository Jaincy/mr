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

public class NspAbilityLogRegionTotalPreProcessor extends Configured implements Tool{
	/*
	 * FATALLOG IMEI日分省统计数据   统计累计总量
	 * 第一步清洗数据
	 * */
	
	public static void main(String[] args) {
		try {
			ToolRunner.run(new NspAbilityLogRegionTotalPreProcessor(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		 if (args.length != 3){
	           System.err.printf("Usage: %s <input><output>",getClass().getSimpleName());
	           ToolRunner.printGenericCommandUsage(System.err);
	           return -1;                  
	          }
		 Configuration conf =getConf();
		 Job job =Job.getInstance(conf, "NspAbilityLogRegionTotalPreProcessor");
		 job.setJarByClass(NspAbilityLogRegionTotalPreProcessor.class);
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileInputFormat.addInputPath(job, new Path(args[1]));
		 FileOutputFormat.setOutputPath(job, new Path(args[2]));
		 
		 job.setMapperClass(NspLogRegionTotalMapper.class);
		 job.setCombinerClass(NspLogRegionTotalRedcuer.class);
		 job.setReducerClass(NspLogRegionTotalRedcuer.class);
		 
		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(IntWritable.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(IntWritable.class);
		 
		 
		 return job.waitForCompletion(true)?0:1; 
	}
	public static class NspLogRegionTotalMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//440000#r#5#i#865062015481546
			//A#r#A#i#865136025680709	1
			String string = value.toString();
			String[] split = string.split("\t");
			String line = split[0];
			//440000#r#5
			String start = line.substring(0,line.indexOf("#i#"));
			//440000
			String areacode = line.substring(0,line.indexOf("#r#"));
			//5
			String requestType = line.substring(line.indexOf("#r#")+3,line.indexOf("#i#"));
			//865062015481546 
			String IMEI = line.substring(line.indexOf("#i#")+3);
			//440000#r#5#i#865062015481546
		
			if(!IMEI.equals("NoIMEI")){
				//正常统计
			context.write(new Text(line), new IntWritable(1));
			//统计不区分地区代码,但是区分requestType
			context.write(new Text("A#r#"+requestType+"#i#"+IMEI), new IntWritable(1));
			//统计不区分requestType,但是区分地区代码的
			context.write(new Text(areacode+"#r#A"+"#i#"+IMEI), new IntWritable(1));
			//统计全部不区分的
			context.write(new Text("A#r#A"+"#i#"+IMEI), new IntWritable(1));
			}
		} 
	}
	
	public static class NspLogRegionTotalRedcuer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			context.write(key, new IntWritable(1));
		}
	}
	
	

}
