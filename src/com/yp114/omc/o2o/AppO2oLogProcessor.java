package com.yp114.omc.o2o;

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
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
 

//import com.yp114.omc.utils.RegionUtil;

public class AppO2oLogProcessor extends Configured implements Tool {

	public static void main(String[] args) {
		/*
		 * 
		 * AppO2o日志 第一步 日志清洗
		 */

		try {
			ToolRunner.run(new Configuration(), new AppO2oLogProcessor(), args);
		} catch (Exception e) {
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {

			System.err.printf("Usage: %s <input><output>", getClass()
					.getSimpleName());

			ToolRunner.printGenericCommandUsage(System.err);

			return -1;
		}

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "AppO2oLogProcessor");
		job.setJarByClass(AppO2oLogProcessor.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(AppO2oLogMapper.class);
		job.setCombinerClass(AppO2oLogReducer.class);
		job.setReducerClass(AppO2oLogReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class AppO2oLogMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			if (line.length() > 0) {
				
				//logId
				String logid = "nologid";
				int logidStartIndex = line.indexOf("logid\":\"");
				int logidEndIndex   = line.indexOf("\",\"requestinfo");
				if (logidStartIndex > 0 && logidEndIndex > 0) {
					logid = line.substring(logidStartIndex + 8,
							logidEndIndex);
				}
				//System.out.println(logid);
				
				// areacode
				String areacode = "noAreacode";
				int areacodeStartIndex = line.indexOf("areacode\":\"");
				int areacodeEndIndex = line.indexOf("\",\"logid\"");
				if (areacodeStartIndex > 0 && areacodeEndIndex > 0) {
					areacode = line.substring(areacodeStartIndex + 11,
							areacodeEndIndex);
				}
				//System.out.println(areacode);
//				if (areacode.length() > 2) {
//					areacode = areacode.substring(0, 2);
//				}
//				String regionCode = RegionUtil.getRegionCode(areacode);
				//System.out.println(regionCode);

				// ID
				String ID = "null";
				int idStartIndex = line.indexOf("\\\"ID\\\":\\\"");
				int idEndIndex = line.indexOf("\\\",\\\"NAME");
				if (idStartIndex > 0 && idEndIndex > 0) {
					ID = line.substring(idStartIndex + 9, idEndIndex);
				} else {
					ID = "null";
				}
				//System.out.println(ID);

				// name
				String name = "noName";
				int nameStartIndex = line.indexOf("\\\"NAME\\\":\\\"");
				int nameEndIndex = line.indexOf("\\\",\\\"NOTE");
				if (nameStartIndex > 0 && nameEndIndex > 0) {
					name = line.substring(nameStartIndex + 11, nameEndIndex);
				}
				//System.out.println(name);

				// note
				String note = "noNote";
				int noteStartIndex = line.indexOf("NOTE\\\":\\\"");
				int noteEndIndex = line.indexOf("\\\",\\\"channelno");
				if (noteStartIndex > 0 && noteEndIndex > 0) {
					note = line.substring(noteStartIndex + 9, noteEndIndex);
				}
				//System.out.println(note);

				// channelno
				String channelno = "noChannel";
				int channelnoStartIndex = line.indexOf("channelno\\\":\\\"");
				int channelnoEndIndex = line.indexOf("\\\",\\\"imei");
				if (channelnoStartIndex > 0 && channelnoEndIndex > 0) {
					channelno = line.substring(channelnoStartIndex + 14,
							channelnoEndIndex);
				}
				//System.out.println(channelno);

				// imei
				String imei = "noImei";
				int imeiStartIndex = line.indexOf("imei\\\":\\\"");
				int imeiEndIndex = line.indexOf("\\\",\\\"queryNum");
				if (imeiStartIndex > 0 && imeiEndIndex > 0) {
					imei = line.substring(imeiStartIndex + 9, imeiEndIndex);
				}
				//System.out.println(imei);
				
				
				// 组装
				char c  =   0x01 ; // 分隔符
//	  			String str =  logid + c +ID + c +imei + c + areacode + c + name+ c + note + c + channelno    ;
	  			//System.out.println(str);
//	  			str = str.replaceAll(" ", "");
//	  			str =str.replaceAll("\t", "");
//	  			str =str.replaceAll("	", "");
		  	    //context.write(new Text(str), new IntWritable(1));
				
		  	    context.write(new Text(ID + "#id#"   + channelno + "#cc#" +  logid + c +imei + c + areacode + c + name+ c + note ), new IntWritable(1));
		  	    context.write(new Text(ID+"#imei#"+imei), new IntWritable(1));
		  	    
		  	    context.write(new Text(ID+"#chl#"+channelno+"#ii#"+imei), new IntWritable(1));
			}

		}
	}

	public static class AppO2oLogReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			 
			context.write(key, new IntWritable(1));
		}
	}

}
