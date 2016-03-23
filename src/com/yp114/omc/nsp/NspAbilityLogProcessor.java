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

import com.yp114.omc.utils.RegionUtil;

public class NspAbilityLogProcessor extends Configured implements Tool{

	
	public static void main(String[] args) {
		/*
		 * 
		 * 能力统计所有工作的第一步清洗
		 * 
		 * */
		
		try {
			ToolRunner.run(new Configuration(),new NspAbilityLogProcessor(), args);
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
		 Configuration conf = getConf();
		 Job job =Job.getInstance(conf, "NspAbilityLogProcessor");
		 job.setJarByClass(NspAbilityLogProcessor.class);
		 DistributedCache.addCacheFile(new Path(args[2]).toUri(), job.getConfiguration());  
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 
		 job.setMapperClass(NspLogMapper.class);
		 job.setCombinerClass(NspLogReducer.class);
		 job.setReducerClass(NspLogReducer.class);
		 
		 job.setMapOutputKeyClass(Text.class);
	     job.setMapOutputValueClass(IntWritable.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(IntWritable.class);
		 
		
		
		 return job.waitForCompletion(true)?0:1; 
	}
	
	public static class NspLogMapper extends Mapper<Object, Text, Text, IntWritable>{
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
	        	  //areacode
	        	  String areacode="noAreacode";	        
	        	  int areacodeStartIndex = line.indexOf("areacode\":\"");
	        	  int areacodeEndIndex = line.indexOf("\",\"countAll\"");
	        		if (areacodeStartIndex > 0 && areacodeEndIndex > 0) {
						areacode = line.substring(areacodeStartIndex + 11,areacodeEndIndex);			
					}
									
					//System.out.println(areacode);
					
					if(areacode.length() > 2){
						areacode = areacode.substring(0, 2);	
					}				
					
					String regionCode = RegionUtil.getRegionCode(areacode);
					  
	        	  
	        		// requesttype                      
	  			int requesttypeStartIndex = line.indexOf("requesttype\":\"");
	  			int requesttypeEndIndex = line.indexOf("\",\"responsecode");

	  			String requesttype = "X";

	  			if (requesttypeStartIndex > 0 && requesttypeEndIndex > 0) {
	  				requesttype = line.substring(requesttypeStartIndex + 14,
	  						requesttypeEndIndex);
	  			}

	  			
	  			
	  			int ImeiStartIndex = line.indexOf("imei=");
	  			int ImeiSEndIndex = line.indexOf(", subjectNum");
	  			String IMEI="NoIMEI";
	  			if(ImeiStartIndex > 0 && ImeiSEndIndex > 0){
	  				IMEI=line.substring(ImeiStartIndex+5, ImeiSEndIndex);
	  				
	  				try {
						if(IMEI.equals("null")||IMEI.equals(" ")||IMEI==null){
							IMEI="NoIMEI";
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						IMEI="NoIMEI";
					}
	  			}else{
	  				
	  				int imeiStartIndex = line.indexOf("\\\"imei\\\":\\\"");
					int imeiEndIndex = line.substring(imeiStartIndex + 11).indexOf("\\\"");
		
		
					if (imeiStartIndex > 0 && imeiEndIndex > 0) {
						IMEI = line.substring(imeiStartIndex + 11, imeiStartIndex + 11 + imeiEndIndex);
					}
	  			}
	  			if(IMEI.equals("NoIMEI")){
	  				int imeiStartIndex = line.indexOf("\\\"imei\\\":\\\"");
					int imeiEndIndex = line.substring(imeiStartIndex + 11).indexOf("\\\"");
		
		
					if (imeiStartIndex > 0 && imeiEndIndex > 0) {
						IMEI = line.substring(imeiStartIndex + 11, imeiStartIndex + 11 + imeiEndIndex);
					}
	  			}

	  			// TEL
	  			int telStartIndex = line.indexOf("queryNum=");
	  			int telEndIndex = line.indexOf(", timestamp=");

	  			String TEL = "noTEL";

	  			if (telStartIndex > 0 && telEndIndex > 0) {
	  				TEL = line.substring(telStartIndex + 9, telEndIndex);
	  				
	  				if(TEL.equals("360_QueryNum_null")){
	  					TEL = "360n";
	  				}
	  			}
	  			
	  			String hz = requesttypeMap.get(requesttype);
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
	  			// 组装
	  			String str=regionCode+ "#r#" + requesttype+"#i#"+IMEI;
	  			str = str.replaceAll(" ", "");
	  			str =str.replaceAll("\t", "");
	  			str =str.replaceAll("	", "");
		  	    context.write(new Text(str), new IntWritable(1));
	  			
			 }
		}
		
	}
	public static class NspLogReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
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
