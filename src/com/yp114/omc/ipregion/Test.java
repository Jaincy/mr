package com.yp114.omc.ipregion;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {



	public static void main(String[] args) {
		String ip = "112.96.66.114";
		//getRegionByIP(ip);
		//IP :1332475 : 1332475 : 1332475 : 59.60.212.187 : 福建省三明市 : 电信
		//Exception 107.170.39.103
		IPSeeker seeker = IPSeeker.getInstance();		
		System.out.println("ip_region :" + ip + " : "+ seeker.getAddress(ip));
		String address = seeker.getAddress(ip);
		System.out.println(address);
		
		//fileIpMatchTest();		
	}
		
	public static void fileIpMatchTest() {	
		
		IPSeeker seeker = IPSeeker.getInstance();
		
		int sumCount = 0;
		int matchCount = 0;
		int ipCount = 0;
		int processCount = 100000;
		
		String startDate = (new Date()).toString();

		try {
			BufferedReader reqReport = new BufferedReader(
					new FileReader(
							"C:\\PROJECTS\\UBSS\\114黄页\\05_维护支持\\201502_OMC信网云割接\\NSP_LOG\\20150320_RegionIP\\2015-03-22_all"));

			BufferedWriter out = new BufferedWriter(new FileWriter("C:\\PROJECTS\\UBSS\\114黄页\\05_维护支持\\201502_OMC信网云割接\\NSP_LOG\\20150320_RegionIP\\2015-03-22_ipRegion"));
	        
			
			String text, line, count, IP, region;

			//&& sumCount < processCount
			
			while ((text = reqReport.readLine()) != null ) {
				sumCount++;

				StringTokenizer itr = new StringTokenizer(text);

				line = itr.nextToken();
				count = itr.nextToken();

				int rtIndex = line.indexOf("#r#");
				int telIndex = line.indexOf("#t#");
				int ipIndex = line.indexOf("#i#");

				IP = line.substring(ipIndex + 3);
				
				if (isIpv4(IP)) {
						
						ipCount++;
						
						try {
							region = seeker.getAddress(IP);
						} catch (Exception se) {
							region = null;
						}
						
						if(region != null){
							matchCount++;
						}
						
						System.out.println("IP :" + sumCount + " : " + ipCount + " : "  + matchCount + " : " + IP + " : " + region);
												
						out.write(IP + " : " + region + "\r\n");
						
					
				}
				
				

			}
			
			String endDate = (new Date()).toString();
			
			System.out.println("ip_region :" + sumCount + " : "	+ ipCount + " : " + matchCount);
			System.out.println("IP time:" + startDate + " : " + endDate);
			
			out.write("IP sum:" + sumCount + " ip: " + ipCount + " match: "  + matchCount );
			out.write("IP time:" + startDate + " : " + endDate  );
			
	        out.close();
	        reqReport.close();

		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	

	public static String getRegionByIP(String ip) {
		Connection connOMC = null;
		PreparedStatement pstmt = null;

		String ipIn = processIP(ip);
		
		String regionCode = null;

		try {

			String driver = "com.mysql.jdbc.Driver";
			String url = "jdbc:mysql://192.168.38.178:3306/omc?useUnicode=true&amp;characterEncoding=utf8";
			String username = "root";
			String password = "114omc";

			Class.forName(driver);
			connOMC = DriverManager.getConnection(url, username, password);

			String sqlQuery = "SELECT a.ip_start , a.ip_end , a.admin_code  "
					+ "  FROM ip_region a where ips <= " + ipIn
					+ " and ipe >= " + ipIn;

			pstmt = connOMC.prepareStatement(sqlQuery);
			ResultSet rs = pstmt.executeQuery();

			while (rs.next()) {
				System.out.println("ip_region :" + rs.getString(1) + " : "
						+ rs.getString(2) + " : " + rs.getString(3));
				
				regionCode = rs.getString(3);

			}

			connOMC.close();
			pstmt.close();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (connOMC != null)
					connOMC.close();
				if (pstmt != null)
					pstmt.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return regionCode;
	}

	public static void importOmcDBTestLocal() {
		Connection connOMC = null;
		PreparedStatement pstmt = null;

		try {

			String driver = "com.mysql.jdbc.Driver";
			String url = "jdbc:mysql://192.168.38.178:3306/omc?useUnicode=true&amp;characterEncoding=utf8";
			String username = "root";
			String password = "114omc";

			Class.forName(driver);
			connOMC = DriverManager.getConnection(url, username, password);

			String sqlQuery = "SELECT a.ip_start , a.ip_end , a.admin_code  "
					+ "  FROM ip_region a where ips is null ";

			pstmt = connOMC.prepareStatement(sqlQuery);
			ResultSet rs = pstmt.executeQuery();

			String sqlUpdate = "";
			String ips, ipe;
			Statement st = connOMC.createStatement();

			int i = 0;

			while (rs.next()) {
				System.out.println("ip_region :" + rs.getString(1) + " : "
						+ rs.getString(2) + " : " + rs.getString(3));

				ips = processIP(rs.getString(1));
				ipe = processIP(rs.getString(2));

				sqlUpdate = "UPDATE ip_region " + "   SET ips = '" + ips + "',"
						+ "       ipe = '" + ipe + "'" + " WHERE ip_start = '"
						+ rs.getString(1) + "'" + "   AND ip_end  = '"
						+ rs.getString(2) + "'" + "   AND admin_code = '"
						+ rs.getString(3) + "'";

				st.addBatch(sqlUpdate);

			}

			st.executeBatch();

			connOMC.close();
			pstmt.close();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (connOMC != null)
					connOMC.close();
				if (pstmt != null)
					pstmt.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static String processIP(String ip) {

		String[] ips = ip.split("\\.");

		long a = Long.parseLong(ips[0]);
		long b = Long.parseLong(ips[1]);
		long c = Long.parseLong(ips[2]);
		long d = Long.parseLong(ips[3]);

		long ipI = (a * 1000000000) + b * 1000000 + c * 1000 + d;

		return String.valueOf(ipI);
	}
	
	
	public static boolean isIpv4(String ipAddress) {

		String ip = "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\."
			    +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
			    +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
			    +"(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)$";

		Pattern pattern = Pattern.compile(ip);
		Matcher matcher = pattern.matcher(ipAddress);
		return matcher.matches();

	}

}
