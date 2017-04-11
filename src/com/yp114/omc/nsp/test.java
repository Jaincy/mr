package com.yp114.omc.nsp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import com.yp114.omc.apponofflog.BaseMr;
import com.yp114.omc.ip.IpSearch;
import com.yp114.omc.ipregion.IPSeeker;
import com.yp114.omc.utils.LatnUtil;
import com.yp114.omc.utils.RegionUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.mortbay.log.Log;

public class test {
    @Test
    public void test1 (){
        char[] chars={'1','q','r'};

        String n=new String(chars,1,2);
//        String[] values = { null, String.Empty, "ABCDE",
//                new String("45", 20), "  \t   ",
//                new String('\u2000', 10) };


        System.out.println(n);
    }


    public static void main(String[] args) throws IOException {
        String line ="dgdgf";
        BaseMr baseMr = new BaseMr(line);
        String requesttype = baseMr.sub("requesttype");
        line = line.trim();
        //过滤掉不包含shopid的记录
//			if (line.contains("{")&&line.substring(line.indexOf("{")).length() > 0)
        if (requesttype.equals("105") || requesttype.equals("170")) {
            try {


                String date_time = line.substring(0, 13);
                String[] split = date_time.split(" ");
                String hour_id = "NoHour";
                String day_id = "Noday";
                if (split.length >= 2) {
                    hour_id = split[split.length - 1];
                    day_id = split[split.length - 2];
                    day_id = day_id.replaceAll("-", "");
                }
                JSONObject jso = new JSONObject(line.substring(line.indexOf("{")));
                // 功能点的Value
                String subjectNum = "noValue";
                // 区域
                String region = "noRegion";
                // 商家ID
                String shopid = "noShopid";
                // 商家名
                String shopname = "noShopname";
                // 渠道号
                String channelno = "noChannelno";
                // 商家名
                String name = "noName";


                if ((line.contains("requestinfo"))
                        && (jso.getString("requestinfo")
                        .startsWith("{"))) {
                    JSONObject requestjso = new JSONObject(
                            jso.getString("requestinfo"));

                    if (line.contains("value")) {
                        subjectNum = requestjso.getString("value");
                    }
                    if (line.contains("region")) {
                        region = requestjso.getString("region");
                    }
                    if (line.contains("shopid")) {
                        shopid = requestjso.getString("shopid");
                    }

                    if (line.contains("shopname")) {
                        shopname = requestjso.getString("shopname");
                    }

                    if (line.contains("channelno")) {
                        channelno = requestjso.getString("channelno");
                        //if (channelno.length() >= 4) {?
                        //channelno = "noChannelno";
                        //}
                    }
                    if (line.contains("name")) {
                        name = requestjso.getString("name");
                    }


                }
                String responsecode = "noErrorCode";
                if ((line.contains("responseinfo"))
                        && (jso.getString("responseinfo")
                        .startsWith("{"))) {
                    JSONObject responseinfo = new JSONObject(
                            jso.getString("responseinfo"));
                    // 请求编码
                    if (line.contains("errorCode")) {
                        responsecode = responseinfo
                                .getString("errorCode");
                    }
                }
                // 请求IP
                String requestip = "noRequestIp";
                if (line.contains("requestip")) {
                    requestip = jso.getString("requestip");
                }
                // 省
                String province = "noProvince";
                // 城市
                String city = "noCity";
                // 县
                String conty = "noConty";
                // 市编码
                String latnCode = "nolatnCode";
                // 省编码
                String provinceCode = "noProvinceCode";

                List<String> zcityList = new ArrayList<String>();
                zcityList.add("澳门");
                zcityList.add("台湾");
                zcityList.add("香港");
                zcityList.add("海南");
                zcityList.add("天津");
                zcityList.add("北京");
                zcityList.add("重庆");
                List<String> zcontyList = new ArrayList<String>();
                zcontyList.add("济源");
                zcontyList.add("仙桃");
                zcontyList.add("潜江");
                zcontyList.add("天门");
                zcontyList.add("神农架");
                zcontyList.add("阿拉尔");
                zcontyList.add("石河子");

                String regex = "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}";
                if (requestip.matches(regex)) {
                    IpSearch finder = IpSearch.getInstance();
                    String result = finder.Get(requestip);
                    result = result.toString().trim().replaceAll("\t", "");
                    String[] splits = result.split("\\|");

                    if (splits.length == 11) {
                        province = splits[2];
                        city = splits[3];
                        conty = splits[4];
                        if (zcityList.contains(province)) {
                            city = province;
                        } else if (zcontyList.contains(conty)) {
                            city = conty;
                        } else if ("".equals(city)) {
                            city = province;
                        }

                        if (province.length() > 2) {
                            province = province.substring(0, 2);
                        }

                        latnCode = LatnUtil.getRegionCode(city);
                        provinceCode = RegionUtil.getRegionCode(province);
                    }


                }
                //"custId\":\"27705162\",\"custName\":
                String custId = baseMr.sub("custId");
                String custName = baseMr.sub("custName");

                String reducekey = day_id + "\t" + hour_id + "\t"
                        + subjectNum + "\t" + region + "\t" + shopid
                        + "\t" + shopname + "\t" + channelno + "\t"
                        + name + "\t" + requestip + "\t" + latnCode
                        + "\t" + provinceCode + "\t" + responsecode + "\t" + requesttype + "\t" + custId + "\t" + custName;
                System.out.println(reducekey);

            } catch (JSONException e) {
                e.printStackTrace();


            }
        }
    }


}
