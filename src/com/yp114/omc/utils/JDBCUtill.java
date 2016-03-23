package com.yp114.omc.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public   class JDBCUtill {
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//System.out.println(proResultFile(args[0]));
		//System.out.println(regionOptToMysql("20150719"));
		
		
	}
	public static Connection getConn() throws Exception, IOException{
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet resultSet = null;
		

		String driver ="com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://192.168.38.178:3306/omc?useUnicode=true&amp;characterEncoding=utf8";
		String username = "root";
		String password ="114omc";
		
		Class.forName(driver);
		return DriverManager.getConnection(url, username, password);
		
	}
	public static void close(PreparedStatement ppst ,Connection conn) throws Exception{
		if (ppst!=null) {
			ppst.close();
		}
		if (conn!=null) {
			conn.close();
		}
		
	}
	}
