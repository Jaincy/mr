package com.yp114.omc.nsp;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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

import com.yp114.omc.ipregion.IPSeeker;
import com.yp114.omc.utils.RegionUtil;

public class NspAbilityLog4yueProcessor extends Configured implements Tool {

	
	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(),new NspAbilityLog4yueProcessor(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf =getConf();
		Job job=Job.getInstance(conf, "NspAbilityLog4yueProcessor");
		job.setJarByClass(NspAbilityLog4yueProcessor.class);
		
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 DistributedCache.addCacheFile(new Path(args[2]).toUri(), job.getConfiguration());  
		 job.setMapperClass(NspAbility4Mapper.class);
		 job.setCombinerClass(NspAbility4Reducer.class);
		 job.setReducerClass(NspAbility4Reducer.class);
		 
		 job.setMapOutputKeyClass(Text.class);
	     job.setMapOutputValueClass(IntWritable.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(IntWritable.class);
		
		
		
		 return job.waitForCompletion(true)?0:1; 
	}

	public static class NspAbility4Mapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private HashMap<String,String> requesttypeMap = new HashMap<String,String>(); 
		@Override
		protected void setup(
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			try{
			Path [] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			  if(null != cacheFiles  && cacheFiles.length > 0){  
                  String line;  
                  String []tokens;  
                  BufferedReader br = new BufferedReader(new FileReader(cacheFiles[0].toString()));  
                  try{  
                      while((line = br.readLine()) != null){  
                          tokens = line.split(",", 2);  
                          requesttypeMap.put(tokens[0], tokens[1]);  
                            
                      }  
                  }finally{  
                      br.close();  
                  }  
              }  
          } catch (IOException e) {  
              e.printStackTrace();  
          }   
		
		}
		
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
		if(line.length() > 0){			
				
				// areacode  ","countAll"
				int areacodeStartIndex = line.indexOf("\"areacode\":\"");
//				int areacodeEndIndex = line.substring(areacodeStartIndex + 12).indexOf("\"");
				int areacodeEndIndex = line.indexOf("\",\"countAll\"");
	
				String areacode = "noAreacode";			
	
				if (areacodeStartIndex > 0 && areacodeEndIndex > 0) {
					areacode = line.substring(areacodeStartIndex + 12,areacodeStartIndex + 12 + areacodeEndIndex);			
				}else{
					int requestipStartIndex = line.indexOf("requestip");
					int requestipEndIndex = line.indexOf("\",\"requesttime\"");
					String ip=line.substring(requestipStartIndex+12,requestipEndIndex);
					
					try {
						IPSeeker seeker = IPSeeker.getInstance();		
						areacode = seeker.getAddress(ip);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				if(areacode.length() > 2){
					areacode = areacode.substring(0, 2);	
				}				
				
				String regionCode = RegionUtil.getRegionCode(areacode);
				
				if(regionCode.equals("XXXXXX")){
					int requestipStartIndex = line.indexOf("requestip");
					int requestipEndIndex = line.indexOf("\",\"requesttime\"");
					String ip=line.substring(requestipStartIndex+12,requestipEndIndex);
					try {
						IPSeeker seeker = IPSeeker.getInstance();		
						areacode = seeker.getAddress(ip);
						if(areacode.length() > 2){
							areacode = areacode.substring(0, 2);	
						}				
						
						regionCode = RegionUtil.getRegionCode(areacode);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				// type  3是来显
				int typeStartIndex = line.indexOf("requesttype");
				//	int typeStartIndex = line.indexOf("\\\"callstype\\\":\\\"");
				int typeEndIndex = line.indexOf("\",\"responsecode\"");
	
				String type = "X";
	
				if (typeStartIndex > 0 && typeEndIndex > 0) {
					type = line.substring(typeStartIndex + 14,typeEndIndex);
				}
				// IMEI
				int imeiStartIndex = line.indexOf("\\\"imei\\\":\\\"");
				int imeiEndIndex = line.substring(imeiStartIndex + 11).indexOf("\\\"");
	
				String IMEI = "noIMEI";
	
				if (imeiStartIndex > 0 && imeiEndIndex > 0) {
					IMEI = line.substring(imeiStartIndex + 11, imeiStartIndex + 11 + imeiEndIndex);
				}
				// 组装
				String keyValue=regionCode + "#r#" + type + "#i#" + IMEI;
				String hz = requesttypeMap.get(type);
				if(hz!=null&&!hz.equals(" ")){
					String keys=regionCode + "#r#" + hz + "#i#" + IMEI;
					keys =keys.replaceAll(" ", "");
					keys =keys.replaceAll("\t", "");
					keys=keys.replaceAll("	", "");
					context.write(new Text(keys), new IntWritable(1));
				}else{
					hz="xx";
					String keys=regionCode + "#r#" + hz + "#i#" + IMEI;
					keys =keys.replaceAll(" ", "");
					keys =keys.replaceAll("\t", "");
					keys=keys.replaceAll("	", "");
					context.write(new Text(keys), new IntWritable(1));
				}
				
				keyValue =keyValue.replaceAll(" ", "");
				keyValue =keyValue.replaceAll("\t", "");
				keyValue=keyValue.replaceAll("	", "");
//				word.set(regionCode + "#a#" + areacode + "#v#" + version + "#o#" + opt + "#c#" + channelno + "#t#" + type + "#i#" + IMEI );
				context.write(new Text(keyValue), new IntWritable(1));
			}
		
		}

	}

	public static class NspAbility4Reducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
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
