package com.yp114.omc.nsp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.yp114.omc.utils.RegionUtil;

public class NspAbilityLogHuaWeiProcessor extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		Job job =Job.getInstance(conf, "NspAbilityLogHuaWeiProcessor");
		job.setJarByClass(NspAbilityLogHuaWeiProcessor.class);
		
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		FileOutputFormat.setCompressOutput(job, true);  
        FileOutputFormat.setOutputCompressorClass(job,BZip2Codec.class);
		job.setMapperClass(OnOffLog4HiveMapper.class);
		job.setCombinerClass(OnOffLog4HiveReducer.class);
		job.setReducerClass(OnOffLog4HiveReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new NspAbilityLogHuaWeiProcessor(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static class OnOffLog4HiveMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString().trim();
			//line = line.replaceAll("\t", "");

			if (line.length() > 0) {

				// date_time
				//String date_time = line.substring(11, 13);
				String date_time = line.substring(0, 13);
				
				String[] split = date_time.split(" ");
				String hour_id= "NoHour";
				String day_id="Noday";
				if(split.length>=2){
					
					System.out.println(split);
					 hour_id = split[split.length-1];
					 day_id = split[split.length-2];
					day_id=day_id.replaceAll("-", "");
				}
				// areacode
				String areacode = "noAreacode";
				try {
					 int areacodeStartIndex = line.indexOf("areacode\":\"");
		        	  int areacodeEndIndex = line.indexOf("\",\"countAll\"");
		        		if (areacodeStartIndex > 0 && areacodeEndIndex > 0) {
							areacode = line.substring(areacodeStartIndex + 11,areacodeEndIndex);			
						}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					InputSplit inputSplit = (InputSplit) context
							.getInputSplit();
					String filename = ((FileSplit) inputSplit).getPath()
							.getName();
					System.out.println("出错的文件名:" + filename);
					System.out.println("出错的行:" + line);
					e.printStackTrace();

				}

				if (areacode.length() > 2) {
					areacode = areacode.substring(0, 2);
				}

				String regionCode = RegionUtil.getRegionCode(areacode);


				// channelno
				String channelno = "noChannelno";
				try {
					int channelnoStartIndex = line
							.indexOf("\"channelno\\\":\\\"");
					int channelnoEndIndex = line.substring(
							channelnoStartIndex + 15).indexOf("\\\"");
					// int channelnoEndIndex = line.indexOf("\\\",\\\"version");
					if (channelnoStartIndex > 0 && channelnoEndIndex > 0) {
						channelno = line.substring(channelnoStartIndex + 15,
								channelnoStartIndex + 15 + channelnoEndIndex);
					}
				} catch (Exception e) {
					InputSplit inputSplit = (InputSplit) context
							.getInputSplit();
					String filename = ((FileSplit) inputSplit).getPath()
							.getName();
					System.out.println("出错的文件名:" + filename);
					System.out.println("出错的行:" + line);
					e.printStackTrace();
				}


				/*
				 * if("3".equals(type)){ type = "LX"; }else{ type = "ZD"; }
				 */

				// IMEI
				String IMEI = "noIMEI";
				try {
					int imeiStartIndex = line.indexOf("\\\"imei\\\":\\\"");
					int imeiEndIndex = line.substring(imeiStartIndex + 11)
							.indexOf("\\\"");
					if (imeiStartIndex > 0 && imeiEndIndex > 0) {
						IMEI = line.substring(imeiStartIndex + 11,
								imeiStartIndex + 11 + imeiEndIndex);
					}
				} catch (Exception e) {
					InputSplit inputSplit = (InputSplit) context
							.getInputSplit();
					String filename = ((FileSplit) inputSplit).getPath()
							.getName();
					System.out.println("出错的文件名:" + filename);
					System.out.println("出错的行:" + line);
					e.printStackTrace();
				}

				
				// IMSI
				String IMSI = "noIMSI";
				try {
					int imsiStartIndex = line.indexOf("\\\"imsi\\\":\\\"");
					// int typeStartIndex =
					// line.indexOf("\\\"callstype\\\":\\\"");
					int imsiEndIndex = line.substring(imsiStartIndex + 11).indexOf("\\\"");
					if (imsiStartIndex > 0 && imsiEndIndex > 0) {
						IMSI = line.substring(imsiStartIndex + 11, imsiStartIndex + 11 + imsiEndIndex);
					}
				} catch (Exception e) {
					InputSplit inputSplit = (InputSplit) context
							.getInputSplit();
					String filename = ((FileSplit) inputSplit).getPath()
							.getName();
					System.out.println("出错的文件名:" + filename);
					System.out.println("出错的行:" + line);
					e.printStackTrace();
				}
				// queryNum
				String queryNum = "noTEL";
				try {
					int telStartIndex = line.indexOf("\\\"queryNum\\\":\\\"");
					// int telStartIndex =
					// line.indexOf("\\\"querynum\\\":\\\"");
					int telEndIndex = line.substring(telStartIndex + 15)
							.indexOf("\\\"");
					if (telStartIndex > 0 && telEndIndex > 0) {
						queryNum = line.substring(telStartIndex + 15, telStartIndex
								+ 15 + telEndIndex);
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					InputSplit inputSplit = (InputSplit) context
							.getInputSplit();
					String filename = ((FileSplit) inputSplit).getPath()
							.getName();
					System.out.println("出错的文件名:" + filename);
					System.out.println("出错的行:" + line);
					e.printStackTrace();
				}

				//subjectNum
				String subjectNum = "NoSubjectNum";
				int subjectNumStartIndex = line.indexOf("subjectNum\\\":\\\"");
				int subjectNumEndIndex = line.indexOf("\\\"}\",\"requestip");
				if (subjectNumStartIndex > 0 && subjectNumEndIndex > 0
						&& subjectNumStartIndex < subjectNumEndIndex)

					subjectNum = line.substring(subjectNumStartIndex + 15,
									subjectNumEndIndex);
				
				//requesttype
				
				String requesttype = "noRequesttype";
				int requesttypeStartIndex = line.indexOf("requesttype\":\"");
	  			int requesttypeEndIndex = line.indexOf("\",\"responsecode");

	  			if (requesttypeStartIndex > 0 && requesttypeEndIndex > 0) {
	  				requesttype = line.substring(requesttypeStartIndex + 14,
	  						requesttypeEndIndex);
	  			}
	  			//responsecode
	  			String responsecode = "noResponsecode";
	  			int responsecodeStartIndex = line.indexOf("responsecode\":\"");
	  			int responsecodeEndIndex = line.indexOf("\",\"userAgent");
	  			
	  			if (responsecodeStartIndex > 0 && responsecodeEndIndex > 0) {
	  				responsecode = line.substring(responsecodeStartIndex + 15,
	  						responsecodeEndIndex);
	  			}
                //requesttime
				
				String requesttime = "noRequestime";
				int requesttimeStartIndex = line.indexOf("requesttime\":\"");
	  			int requesttimeEndIndex = line.indexOf("\",\"requesttype");

	  			if (requesttimeStartIndex > 0 && requesttimeEndIndex > 0) {
	  				requesttime = line.substring(requesttimeStartIndex + 14,
	  						requesttimeEndIndex);
	  			}
	  			
				
				// 组装
				String keyValue = day_id + "\t" +hour_id + "\t" +regionCode + "\t" + channelno + "\t" + IMEI
						+ "\t" + IMSI + "\t" + queryNum + "\t" + subjectNum + "\t" + requesttype+ "\t" + responsecode
						+"\t"+requesttime;
/*				String keyValue = regionCode + "#v#" + version + "#o#" + opt
						+ "#c#" + channelno + "#t#" + type + "#i#" + IMEI;
*/				word.set(keyValue);
				// word.set(regionCode + "#a#" + areacode + "#v#" + version +
				// "#o#" + opt + "#c#" + channelno + "#t#" + type + "#i#" + IMEI
				// );
				context.write(word, one);

			}
		}
	}

	public static class OnOffLog4HiveReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

}
