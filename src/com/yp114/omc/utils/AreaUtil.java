package com.yp114.omc.utils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.sql.rowset.JdbcRowSet;

public  class AreaUtil {
	public static String getCode(String area) throws Exception, Exception{
		Connection conn=JDBCUtill.getConn();
		String sql="select latn_id from td_code_tele_area where latn_name=?";
		PreparedStatement ppst=conn.prepareStatement(sql);
		ppst.setString(1, area);
		ResultSet rs= ppst.executeQuery();
		String code=null;
	    while(rs.next()){
	    	code=rs.getString("latn_id");
	    }
	    JDBCUtill.close(ppst, conn);
		
		
		return code;
	}
	public static void main(String[] args) throws Exception {
		System.out.println(getCode("北京"));
	}

}
