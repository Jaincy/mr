package com.yp114.omc.tel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class HotTelphoneProcessor extends Configured implements Tool{

		//根电话号码出现的次数排序
	public static void main(String[] args) {
		try {
			ToolRunner.run(new HotTelphoneProcessor(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf =getConf();
		Job job =Job.getInstance(conf, "HotTelphoneProcessor");
		job.setJarByClass(HotTelphoneProcessor.class);
		job.setMapperClass(TelMapper.class);
		job.setReducerClass(TelReducer.class);
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setGroupingComparatorClass(GroupingComparator.class);
		
		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		
		
		return job.waitForCompletion(true)?0:1;
	}
	
	
	
	
	public static class TelMapper extends Mapper<Object, Text, IntPair, IntWritable>{
		
		private final IntPair key = new IntPair();
		private final IntWritable value = new IntWritable();
		@Override
		protected void map(Object inkey, Text value,
				Mapper<Object, Text, IntPair, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String string = value.toString();
			String[] split = string.split("\t");
			String line = split[0];
			String count =split[1];
			int parseInt = Integer.parseInt(count);
			key.setFirst(line);
			key.setSecond(parseInt);
			context.write(key, new IntWritable(parseInt));
		
		}
		
		
	}
	
	public static class TelReducer extends Reducer<IntPair, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(IntPair key, Iterable<IntWritable> values,
				Reducer<IntPair, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (IntWritable intWritable : values) {
				
				context.write(new Text(key.getFirst()), intWritable);
			}
		}
	}
	
	public static class IntPair implements WritableComparable<IntPair>{
		
		private String first="";
		private int second=0;
		
		

		public String getFirst() {
			return first;
		}

		public void setFirst(String first) {
			this.first = first;
		}

		public int getSecond() {
			return second;
		}

		public void setSecond(int second) {
			this.second = second;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			first=in.readUTF();
			second=in.readInt();
			
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(first);;
			out.writeInt(second);
		}
		
		
		@Override
		public int hashCode() {
			// TODO Auto-generated method stub
			return first.hashCode()+second+"".hashCode();
		}

		@Override
		public boolean equals(Object regiht) {
			// TODO Auto-generated method stub
			if(regiht instanceof IntPair){
				IntPair r =(IntPair) regiht;
				return r.first==first && r.second==second;
			}else{
				return false;
			}
		}

		@Override
		public int compareTo(IntPair o) {
			// TODO Auto-generated method stub
			if(second !=o.second){
				return o.second-second;
			}else if(first!=o.first){
				//return first.hashCode()-o.first.hashCode();
				return o.first.compareTo(first);
			}else{
				return 0;
			}
			
		}
		
	}
	
	public static class GroupingComparator implements RawComparator<IntPair>{

		@Override
		public int compare(IntPair o1, IntPair o2) {
			// TODO Auto-generated method stub
			int second1 = o1.getSecond();
			int second2 = o2.getSecond();
			return second2-second1;
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2,
				int s2, int l2) {
			// TODO Auto-generated method stub
			return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8, b2, s2, Integer.SIZE/8);
		}
		
		
	}
	
	
	
}
