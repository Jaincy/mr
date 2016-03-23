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

public class NspAbilityLogDayRequestProcessor extends Configured implements Tool{
	/*
	 * FATALLOG 接口请求日统计
	 * 
	 * */
	
	public static void main(String[] args) {
		try {
			ToolRunner.run(new NspAbilityLogDayRequestProcessor(), args);
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
		 Job job = Job.getInstance(conf, "NspAbilityLogDayRequestProcessor");
		 job.setJarByClass(NspAbilityLogDayRequestProcessor.class);
		 
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 
		 job.setMapperClass(NspLogDayRequestMapper.class);
		 job.setReducerClass(NspLogDayRequestReducer.class);
		 job.setReducerClass(NspLogDayRequestReducer.class);
		 
		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(IntWritable.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(IntWritable.class);
		 
		 
		 
		 return job.waitForCompletion(true)?0:1; 
	}
	
	public static class NspLogDayRequestMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		    //140000#r#5#i#359535053009959
			String string = value.toString();
			String[] split = string.split("\t");
			String line =split[0];
			int count =Integer.parseInt(split[1]);
			String start = line.substring(0,line.indexOf("#i#"));
			String IMEI  = line.substring(line.indexOf("#i#")+3);
			if(IMEI.equals("NoIMEI")||IMEI.equals("null")){
				context.write(new Text(start+"#i#NoIMEI"), new IntWritable(count));
			}else{
				context.write(new Text(start+"#i#IMEI"), new IntWritable(count));
			}
			context.write(new Text(start+"#i#A"), new IntWritable(count));
		
		}
		
		
		
	}
	
	public static class NspLogDayRequestReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
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
