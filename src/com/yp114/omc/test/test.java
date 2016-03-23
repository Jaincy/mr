package com.yp114.omc.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import com.yp114.omc.utils.RegionUtil;

public class test {
	
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(new File("C:\\Users\\wxyu\\Desktop\\Hadoop\\bdc.log")));
		String readLine = null;
		while ((readLine = br.readLine())!=null) {
			String line = readLine;
			System.out.println(line);
            if(line.length() > 0){			
    			
				// areacode
				int areacodeStartIndex = line.indexOf("\"areacode\":\"");
				int areacodeEndIndex = line.substring(areacodeStartIndex + 12).indexOf("\"");
	
				String areacode = "noAreacode";			
	
				if (areacodeStartIndex > 0 && areacodeEndIndex > 0) {
					areacode = line.substring(areacodeStartIndex + 12,areacodeStartIndex + 12 + areacodeEndIndex);			
				}
								
				//System.out.println(areacode);
				
				if(areacode.length() > 2){
					areacode = areacode.substring(0, 2);	
				}				
				
				String regionCode = RegionUtil.getRegionCode(areacode);
				
								
				// version
				int versionStartIndex = line.indexOf("\\\"version\\\":\\\"");
				int versionEndIndex = line.substring(versionStartIndex + 14).indexOf("\\\"");
									
				String version = "noVersion";			
	
				if (versionStartIndex > 0 && versionEndIndex > 0) {
					version = line.substring(versionStartIndex + 14, versionStartIndex + 14 + versionEndIndex);			
				}
				
				//channelno
				int channelnoStartIndex = line.indexOf("\"channelno\\\":\\\"");
				System.out.println("channelnoStartIndex  "+channelnoStartIndex);
				int channelnoEndIndex = line.substring(channelnoStartIndex + 15).indexOf("\\\"");
				//int channelnoEndIndex = line.indexOf("\\\",\\\"version");
				String channelno = "noChannelno";			
	
				if (channelnoStartIndex > 0 && channelnoEndIndex > 0) {
					channelno = line.substring(channelnoStartIndex + 15, channelnoStartIndex + 15 + channelnoEndIndex);			
					//	channelno = line.substring(channelnoStartIndex+15,channelnoEndIndex);
				}
				
				
	
				// type  3是来显
				int typeStartIndex = line.indexOf("\\\"type\\\":\\\"");
				//	int typeStartIndex = line.indexOf("\\\"callstype\\\":\\\"");
				int typeEndIndex = line.substring(typeStartIndex + 11).indexOf("\\\"");
	
				String type = "X";
	
				if (typeStartIndex > 0 && typeEndIndex > 0) {
					type = line.substring(typeStartIndex + 11, typeStartIndex + 11 + typeEndIndex);
				}
				
				/*if("3".equals(type)){
					type = "LX";
				}else{
					type = "ZD";
				}*/
	
				// IMEI
				int imeiStartIndex = line.indexOf("\\\"imei\\\":\\\"");
				int imeiEndIndex = line.substring(imeiStartIndex + 11).indexOf("\\\"");
	
				String IMEI = "noIMEI";
	
				if (imeiStartIndex > 0 && imeiEndIndex > 0) {
					IMEI = line.substring(imeiStartIndex + 11, imeiStartIndex + 11 + imeiEndIndex);
				}
				
	
				// TEL
				int telStartIndex = line.indexOf("\\\"queryNum\\\":\\\"");
				//	int telStartIndex = line.indexOf("\\\"querynum\\\":\\\"");
				int telEndIndex = line.substring(telStartIndex + 15).indexOf("\\\"");
				
				String TEL = "noTEL";
	
				if (telStartIndex > 0 && telEndIndex > 0) {
					TEL = line.substring(telStartIndex + 15, telStartIndex + 15 + telEndIndex);				
				}
				
				// opt
	            String opt = "noCT";
			                                  	//133/153/180/181/189/177
				if (TEL.length() == 11 &&
						(TEL.startsWith("177") ||TEL.startsWith("181") ||TEL.startsWith("133") || TEL.startsWith("153") || TEL.startsWith("180") || TEL.startsWith("189"))  ) {
					opt = "CT";
				}
				String keyValue=regionCode + "#a#" + "XX" + "#v#" + version + "#o#" + opt + "#c#" + channelno + "#t#" + type + "#i#" + IMEI;
				System.err.println(keyValue);
}
		
		
		}
		
	}

}
