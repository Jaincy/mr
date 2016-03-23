package com.yp114.omc.tel;

public class test {
	public static void main(String[] args) {
		
		String str="1413686783";
		String str1="1413686782";
		System.out.println(str1.hashCode()-str.hashCode());
		System.out.println(str1.compareTo(str));
	}

}
