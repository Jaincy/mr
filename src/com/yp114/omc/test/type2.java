package com.yp114.omc.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.yp114.omc.utils.RegionUtil;


public class type2 {

	
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();			
		

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString().trim();
			String str=line;
			
			if(line.length() > 0){			
				
				// date_time
			
				// type  3是来显
				int typeStartIndex = line.indexOf("\\\"type\\\":\\\"");
				int typeEndIndex = line.substring(typeStartIndex + 11).indexOf("\\\"");
	
				String type = "X";
	
				if (typeStartIndex > 0 && typeEndIndex > 0) {
					type = line.substring(typeStartIndex + 11, typeStartIndex + 11 + typeEndIndex);
				}
				
				/*if("3".equals(type)){
					type = "LX";
				}else{
					type = "ZD";
				}*/
	
				// IMEI
				
				
	
				
				
				
				
	
				// 组装
//				String keyValue=regionCode + "#a#" + "XX" + "#v#" + version + "#o#" + opt + "#c#" + channelno + "#t#" + type + "#i#" + IMEI;
				
				if(type.equals("2")||type.equals("3")){
					word.set(str);
//					word.set(regionCode + "#a#" + areacode + "#v#" + version + "#o#" + opt + "#c#" + channelno + "#t#" + type + "#i#" + IMEI );
					context.write(word, one);
				}
				
			
			}

		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		protected void reduce(Text arg0, Iterable<IntWritable> arg1,
				Reducer<Text, IntWritable, Text, IntWritable>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.reduce(arg0, arg1, arg2);
		}
	}

	

	public static void main(String[] args) throws Exception {
		/*
		 * String line =
		 * "2015-01-07 11:00:00 {\"requestinfo\":\"Request360 [interfaceUserName=null, queryNum=18340059885, timestamp=1420599819476, location=null, appKey=yellowpage114sdkfor360, imsi=460078131637636, imei=354833052839608, subjectNum=01082694419, userAgent=userAgent, version=-1]\",\"requestip\":\"28107760\","
		 * ;
		 * 
		 * int imsiStartIndex = line.indexOf("imsi="); int imsiEndIndex =
		 * line.indexOf(", imei="); String IMSI = line.substring(imsiStartIndex
		 * + 5, imsiEndIndex);
		 * 
		 * System.out.println(IMSI);
		 */

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "nap log day tel");
		job.setJarByClass(type2.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//job.setOutputFormatClass(GbkOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
