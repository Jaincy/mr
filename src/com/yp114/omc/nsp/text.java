package com.yp114.omc.nsp;

import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class text {
	public static void main(String[] args) {
		//String linew ="XXXXXX#a#XX#v#7.1.3.0ctch1#o#noCT#c#1018#t#3#i#99000467088023	3";
		String line = "440000#r#5#i#865062015481546";
		//000000#a#中国#v#7.1.0.0ctch1#o#noCT#c#100#t#0#i#353570052188781	1
		//000000#a#中国#v#7.1.0.0ctch1#o#noCT#c#100#t#0#i#353570052188781	1
		//440000#r#5
		String start = line.substring(0,line.indexOf("#i#"));
		//440000
		String areacode = line.substring(0,line.indexOf("#r#"));
		//5
		String requestType = line.substring(line.indexOf("#r#")+3,line.indexOf("#i#"));
		//865062015481546 
		String IMEI = line.substring(line.indexOf("#i#")+3);
		//440000#r#5#i#865062015481546
		//正常统计
		//context.write(new Text(line), new IntWritable(1));
		//统计不区分地区代码,但是区分requestType
		System.out.println("A#r#"+requestType+"#i#"+IMEI);
		//context.write(new Text("A#r#"+requestType+"#i#"+IMEI), new IntWritable(1));
		//统计不区分requestType,但是区分地区代码的
		System.out.println(areacode+"#r#A"+"#i#"+IMEI);
		//context.write(new Text(areacode+"#r#A"+"#i#"+IMEI), new IntWritable(1));
		//统计全部不区分的
		System.out.println("A#r#A"+"#i#"+IMEI);
		//context.write(new Text("A#r#A"+"#i#"+IMEI), new IntWritable(1));
		System.out.println(line);
	}

}
