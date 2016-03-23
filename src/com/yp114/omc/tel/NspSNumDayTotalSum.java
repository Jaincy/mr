package com.yp114.omc.tel;

import java.io.IOException;
import java.util.StringTokenizer;

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

public class NspSNumDayTotalSum extends Configured implements Tool{
	public static void main(String args[]){
		try {
			ToolRunner.run(new NspSNumDayTotalSum(), args);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		  conf.set("dfs.client.block.write.replace-datanode-on-failure.policy",
	                "NEVER"
	        ); 

	conf.set("dfs.client.block.write.replace-datanode-on-failure.enable",
	                "true"
	        ); 
		Job job = Job.getInstance(conf, "NspSNumDayTotalSum");
		job.setJarByClass(NspSNumDayTotalSum.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setMapperClass(CombineMapper.class);
		job.setCombinerClass(CombineReducer.class);
		job.setReducerClass(CombineReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
    public static class CombineMapper extends Mapper<Object,Text,Text,IntWritable>{
    	public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
    		IntWritable count = new IntWritable();
    	    Text word = new Text();
			String line = value.toString();
			String itr[] = line.split("\t");
			String keyStr="";
			String count1 = itr[itr.length-1];
			if(itr.length>2){
				int i= 0;
				while(i<itr.length-1){
					keyStr += itr[i];
				}
			}else{
				keyStr = itr[0];
			}
			word.set(keyStr);
		    count.set(Integer.parseInt(count1));
			context.write(word,count);
		}
    }
    
    public static class CombineReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
    		IntWritable result = new IntWritable();
			int sum = 0;
			for(IntWritable val:values){
				sum += val.get();
			}
			result.set(sum);
			context.write(key,result);
		}
    }
}
