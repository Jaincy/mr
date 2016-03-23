package com.yp114.omc.tel;

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

public class NspSNumDayChekFirstClear extends Configured implements Tool {
	public static void main(String args[]){
		try {
			ToolRunner.run(new NspSNumDayChekFirstClear(), args);
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
		Job job = Job.getInstance(conf, "NspSNumDayChekFirstClear");
		job.setJarByClass(NspSNumDayChekFirstClear.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
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
    		IntWritable one = new IntWritable(1);
    	    Text word = new Text();
			String line = value.toString();
			String regioncode = line.substring(0,line.indexOf("#r#"));
			String requestType = line.substring(line.indexOf("#r#")+3,line.indexOf("#s#"));
			String yesOrNo = line.substring(line.indexOf("#y#")+3,line.indexOf("\t"));
			String subjectNum = line.substring(line.indexOf("#s#")+3,line.indexOf("#c#"));
			word.set(regioncode+"#r#"+requestType+"#s#"+subjectNum+"#y#"+yesOrNo);
			context.write(word, one);
			word.set("A#r#"+requestType+"#s#"+subjectNum+"#y#"+yesOrNo);
			context.write(word, one);
			word.set(regioncode+"#r#A#s#"+subjectNum+"#y#"+yesOrNo);
			context.write(word, one);
			word.set("A#r#A#s#"+subjectNum+"#y#"+yesOrNo);
			context.write(word,one);
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
