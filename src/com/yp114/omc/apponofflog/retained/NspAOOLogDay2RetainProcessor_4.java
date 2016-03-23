package com.yp114.omc.apponofflog.retained;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

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


public class NspAOOLogDay2RetainProcessor_4 extends Configured implements Tool{

	//4，合并上两步的结果  ， 相同key 中 做除运算
	public static void main(String[] args) {
		try {
			ToolRunner.run(new NspAOOLogDay2RetainProcessor_4(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "NspAOOLogDay2RetainProcessor_4");
		job.setJarByClass(NspAOOLogDay2RetainProcessor_4.class);
		job.setMapperClass(AOODay2Mapper_4.class);
		job.setReducerClass(AOODay2Reducer_4.class);
		
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileInputFormat.addInputPath(job, new Path(args[1]));
		  FileOutputFormat.setOutputPath(job, new Path(args[2]));
			 job.setMapOutputKeyClass(Text.class);
		     job.setMapOutputValueClass(Text.class);
			 // job.setMapOutputValueClass(IntWritable.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class);
//		  job.setOutputValueClass(IntWritable.class);
		  
		  return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class AOODay2Mapper_4 extends Mapper<Object, Text, Text, Text>{
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String string = value.toString();
			String[] split = string.split("\t");
			String line = split[0];
			//int count = Integer.parseInt(split[1]);
			
			context.write(new Text(line), new Text(split[1]));
			
		}
		
	}
	
	public static class AOODay2Reducer_4 extends Reducer<Text, Text, Text, Text>{
		
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String result="";
			for (Text text : values) {
				result+=text.toString()+"#r#";
			}
			String[] split = result.split("#r#");
			String strA="";
			String strB="";
			if(split.length>1&&split[0].endsWith("A")&&split[1].endsWith("B")){
				strA=split[0].substring(0, split[0].length()-1);
				strB=split[1].substring(0, split[1].length()-1);
			}else if(split.length>1&&split[0].endsWith("B")&&split[1].endsWith("A")){
				strB=split[0].substring(0, split[0].length()-1);
				strA=split[1].substring(0, split[1].length()-1);
			}else{
				strA="0";
				strB="1";
			}
			int a=Integer.parseInt(strA);
			int b=Integer.parseInt(strB);
			//		double c=(double)a/b;
			double c=(double)a/b*100.0;
			DecimalFormat df2  = new DecimalFormat("0.00");
			String format = df2.format(c);
			context.write(new Text(key), new Text(format));
			
		}
	}

}
