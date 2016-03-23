package com.yp114.omc.apponofflog;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.yp114.omc.utils.RegionUtil;


public class NspAppOnOffLog4yueProcessor {

	
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();			
		

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString().trim();
			
			
			if(line.length() > 0){			
				
				// date_time
				String date_time = line.substring(11, 13);		
			
				// areacode
				String areacode = "noAreacode";			
				try {
					int areacodeStartIndex = line.indexOf("\"areacode\":\"");
					int areacodeEndIndex = line.substring(areacodeStartIndex + 12).indexOf("\"");
					if (areacodeStartIndex > 0 && areacodeEndIndex > 0) {
						areacode = line.substring(areacodeStartIndex + 12,areacodeStartIndex + 12 + areacodeEndIndex);			
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					InputSplit inputSplit=(InputSplit)context.getInputSplit();
					String filename=((FileSplit)inputSplit).getPath().getName();
					System.out.println("出错的文件名:"+filename);
					System.out.println("出错的行:"+line);
					e.printStackTrace();
					
				}
								
				//System.out.println(areacode);
				
				if(areacode.length() > 2){
					areacode = areacode.substring(0, 2);	
				}				
				
				String regionCode = RegionUtil.getRegionCode(areacode);
				
								
				// version
				String version = "noVersion";			
				try {
				int versionStartIndex = line.indexOf("\\\"version\\\":\\\"");
				int versionEndIndex = line.substring(versionStartIndex + 14).indexOf("\\\"");
				if (versionStartIndex > 0 && versionEndIndex > 0) {
					version = line.substring(versionStartIndex + 14, versionStartIndex + 14 + versionEndIndex);			
				}
				} catch (Exception e) {
					// TODO: handle exception
					InputSplit inputSplit=(InputSplit)context.getInputSplit();
					String filename=((FileSplit)inputSplit).getPath().getName();
					System.out.println("出错的文件名:"+filename);
					System.out.println("出错的行:"+line);
					e.printStackTrace();
				}
				
				//channelno
				String channelno = "noChannelno";	
				try {
				int channelnoStartIndex = line.indexOf("\"channelno\\\":\\\"");
				int channelnoEndIndex = line.substring(channelnoStartIndex + 15).indexOf("\\\"");
				//int channelnoEndIndex = line.indexOf("\\\",\\\"version");
				if (channelnoStartIndex > 0 && channelnoEndIndex > 0) {
					channelno = line.substring(channelnoStartIndex + 15, channelnoStartIndex + 15 + channelnoEndIndex);			
				}
				} catch (Exception e) {
					InputSplit inputSplit=(InputSplit)context.getInputSplit();
					String filename=((FileSplit)inputSplit).getPath().getName();
					System.out.println("出错的文件名:"+filename);
					System.out.println("出错的行:"+line);
					e.printStackTrace();
				}
				
	
				// type  3是来显
				String type = "X";
				
				try {
					//int typeStartIndex = line.indexOf("\\\"type\\\":\\\"");
						int typeStartIndex = line.indexOf("\\\"callstype\\\":\\\"");
					int typeEndIndex = line.substring(typeStartIndex + 11).indexOf("\\\"");
					if (typeStartIndex > 0 && typeEndIndex > 0) {
						type = line.substring(typeStartIndex + 11, typeStartIndex + 11 + typeEndIndex);
					}
				} catch (Exception e) {
					InputSplit inputSplit=(InputSplit)context.getInputSplit();
					String filename=((FileSplit)inputSplit).getPath().getName();
					System.out.println("出错的文件名:"+filename);
					System.out.println("出错的行:"+line);
					e.printStackTrace();
				}
				
				/*if("3".equals(type)){
					type = "LX";
				}else{
					type = "ZD";
				}*/
	
				// IMEI
				String IMEI = "noIMEI";
				try {
					int imeiStartIndex = line.indexOf("\\\"imei\\\":\\\"");
					int imeiEndIndex = line.substring(imeiStartIndex + 11).indexOf("\\\"");
					if (imeiStartIndex > 0 && imeiEndIndex > 0) {
						IMEI = line.substring(imeiStartIndex + 11, imeiStartIndex + 11 + imeiEndIndex);
					}
				} catch (Exception e) {
					InputSplit inputSplit=(InputSplit)context.getInputSplit();
					String filename=((FileSplit)inputSplit).getPath().getName();
					System.out.println("出错的文件名:"+filename);
					System.out.println("出错的行:"+line);
					e.printStackTrace();
				}
				
	
				// TEL
				String TEL = "noTEL";
				try {
					//int telStartIndex = line.indexOf("\\\"queryNum\\\":\\\"");
						int telStartIndex = line.indexOf("\\\"querynum\\\":\\\"");
					int telEndIndex = line.substring(telStartIndex + 15).indexOf("\\\"");
					if (telStartIndex > 0 && telEndIndex > 0) {
						TEL = line.substring(telStartIndex + 15, telStartIndex + 15 + telEndIndex);				
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					InputSplit inputSplit=(InputSplit)context.getInputSplit();
					String filename=((FileSplit)inputSplit).getPath().getName();
					System.out.println("出错的文件名:"+filename);
					System.out.println("出错的行:"+line);
					e.printStackTrace();
				}
				
				// opt
	            String opt = "noCT";
			                                  	//133/153/180/181/189/177
				if (TEL.length() == 11 &&
						(TEL.startsWith("177") ||TEL.startsWith("181") ||TEL.startsWith("133") || TEL.startsWith("153") || TEL.startsWith("180") || TEL.startsWith("189"))  ) {
					opt = "CT";
				}
				
	
				// 组装
				String keyValue=regionCode + "#a#" + "XX" + "#v#" + version + "#o#" + opt + "#c#" + channelno + "#t#" + type + "#i#" + IMEI;
				keyValue =keyValue.replaceAll(" ", "");
				keyValue =keyValue.replaceAll("\t", "");
				keyValue =keyValue.replaceAll("	", "");
				word.set(keyValue);
//				word.set(regionCode + "#a#" + areacode + "#v#" + version + "#o#" + opt + "#c#" + channelno + "#t#" + type + "#i#" + IMEI );
				context.write(word, one);
			
			}

		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
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
		job.setJarByClass(NspAppOnOffLog4yueProcessor.class);
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
