package com.yp114.omc.nsp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import com.yp114.omc.ipregion.IPSeeker;
import com.yp114.omc.utils.RegionUtil;

public class test {
	

	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(new File("C:\\Users\\wxyu\\Desktop\\Hadoop\\bdc.log")));
		String readLine = null;
		while ((readLine = br.readLine())!=null) {
			String line = readLine;
		if(line.length() > 0){			
				
				// areacode  ","countAll"
				int areacodeStartIndex = line.indexOf("\"areacode\":\"");
//				int areacodeEndIndex = line.substring(areacodeStartIndex + 12).indexOf("\"");
				int areacodeEndIndex = line.indexOf("\",\"countAll\"");
	
				String areacode = "noAreacode";			
	
				if (areacodeStartIndex > 0 && areacodeEndIndex > 0) {
					areacode = line.substring(areacodeStartIndex + 12,areacodeStartIndex + 12 + areacodeEndIndex);			
				}else{
					int requestipStartIndex = line.indexOf("requestip");
					int requestipEndIndex = line.indexOf("\",\"requesttime\"");
					String ip=line.substring(requestipStartIndex+12,requestipEndIndex);
					
					try {
						IPSeeker seeker = IPSeeker.getInstance();		
						areacode = seeker.getAddress(ip);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				if(areacode.length() > 2){
					areacode = areacode.substring(0, 2);	
				}				
				
				String regionCode = RegionUtil.getRegionCode(areacode);
				
				if(regionCode.equals("XXXXXX")){
					int requestipStartIndex = line.indexOf("requestip");
					int requestipEndIndex = line.indexOf("\",\"requesttime\"");
					String ip=line.substring(requestipStartIndex+12,requestipEndIndex);
					try {
						IPSeeker seeker = IPSeeker.getInstance();		
						areacode = seeker.getAddress(ip);
						if(areacode.length() > 2){
							areacode = areacode.substring(0, 2);	
						}				
						
						regionCode = RegionUtil.getRegionCode(areacode);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				// type  3是来显
				int typeStartIndex = line.indexOf("requesttype");
				//	int typeStartIndex = line.indexOf("\\\"callstype\\\":\\\"");
				int typeEndIndex = line.indexOf("\",\"responsecode\"");
	
				String type = "X";
	
				if (typeStartIndex > 0 && typeEndIndex > 0) {
					type = line.substring(typeStartIndex + 14,typeEndIndex);
				}
				// IMEI
				int imeiStartIndex = line.indexOf("\\\"imei\\\":\\\"");
				int imeiEndIndex = line.substring(imeiStartIndex + 11).indexOf("\\\"");
	
				String IMEI = "noIMEI";
	
				if (imeiStartIndex > 0 && imeiEndIndex > 0) {
					IMEI = line.substring(imeiStartIndex + 11, imeiStartIndex + 11 + imeiEndIndex);
				}
				// 组装
				String keyValue=regionCode + "#r#" + type + "#i#" + IMEI;
				keyValue =keyValue.replaceAll(" ", "");
				//word.set(keyValue);
//				word.set(regionCode + "#a#" + areacode + "#v#" + version + "#o#" + opt + "#c#" + channelno + "#t#" + type + "#i#" + IMEI );
			//	context.write(word, one);
			System.out.println(keyValue);
			}
		
		
		}
		
	}

}
