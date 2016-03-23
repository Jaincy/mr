package com.yp114.omc.apponofflog.retained;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

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

public class NspAOOLogDay2RetainProcessor_1 extends Configured implements Tool{
	
	
	//1，汇总 昨天和今天数据，
	public static void main(String[] args) {
		try {
			ToolRunner.run(new NspAOOLogDay2RetainProcessor_1(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "NspAOOLogDay2RetainProcessor_1");
		job.setJarByClass(NspAOOLogDay2RetainProcessor_1.class);
		job.setMapperClass(AOODay2Mapper.class);
		job.setReducerClass(AOODay2Reducer.class);
		///user/omc/${deal_date}/${deal_date}_request
		String string = args[0];
		int index = Integer.parseInt(args[1]);
		String[] split = string.split("/");
		String date = split[split.length-2];
		String specifiedDayBefore = getSpecifiedDayBefore(date,index);
		String path=string.replaceAll(date, specifiedDayBefore);

		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileInputFormat.addInputPath(job, new Path(path));
		  FileOutputFormat.setOutputPath(job, new Path(args[2]));
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(IntWritable.class);
		  
		  return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static String getSpecifiedDayBefore(String specifiedDay,int index){


	    Calendar c = Calendar.getInstance();
	    Date date=null;
	    try {
			date = new SimpleDateFormat("yyyyMMdd").parse(specifiedDay);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    c.setTime(date);
	    int day=c.get(Calendar.DATE);
	    c.set(Calendar.DATE,day-index);
	    String dayBefore=new SimpleDateFormat("yyyyMMdd").format(c.getTime());
	    return dayBefore;

	    }
	public static class AOODay2Mapper extends Mapper<Object, Text, Text, IntWritable>{
		
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String string = value.toString();
			String[] split = string.split("\t");
			String line = split[0];
			int count = Integer.parseInt(split[1]);
			
			context.write(new Text(line), new IntWritable(count));
			
		}
		
	}
	
	public static class AOODay2Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
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
