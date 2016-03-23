package com.yp114.omc.test;

import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class text {
	public static void main(String[] args) {
		//String linew ="XXXXXX#a#XX#v#7.1.3.0ctch1#o#noCT#c#1018#t#3#i#99000467088023	3";
		String line = "110000#r#5#i#I	KB�g�;��p�-�Z		1";
		//000000#a#中国#v#7.1.0.0ctch1#o#noCT#c#100#t#0#i#353570052188781	1
		//000000#a#中国#v#7.1.0.0ctch1#o#noCT#c#100#t#0#i#353570052188781	1
		//line=	line.replaceAll("	", "");
		String[] split = line.split("\t");
		System.out.println(split.length);
		for (String string : split) {
			System.out.println(string);
		}
	}

}
