package com.yp114.omc.apponofflog;

import com.yp114.omc.ip.IpSearch;
import com.yp114.omc.utils.LatnUtil;
import com.yp114.omc.utils.RegionUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Ander on 2016/3/21.
 */
public class BaseMr {
    static List<String> zcityList = new ArrayList<String>();
    static List<String> zcontyList = new ArrayList<String>();
    String line;

    static {
        zcityList.add("澳门");
        zcityList.add("台湾");
        zcityList.add("香港");
        zcityList.add("海南");
        zcityList.add("天津");
        zcityList.add("北京");
        zcityList.add("重庆");

        zcontyList.add("济源");
        zcontyList.add("仙桃");
        zcontyList.add("潜江");
        zcontyList.add("天门");
        zcontyList.add("神农架");
        zcontyList.add("阿拉尔");
        zcontyList.add("石河子");
        /* String[] zcity={"台湾","香港","澳门","海南","天津","北京","重庆"};
            String[] zconty={"济源","仙桃","潜江","天门","神农架","阿拉尔","石河子"};*/
    }


    public String sub(String str) {
        String ret = "no" + str;

        try {
            if (line.contains("\"" + str + "\"")) {

                int start = line.indexOf("\"" + str + "\":\"") + 4 + str.length();
                int end = start + line.substring(start).indexOf("\"");
                ret = line.substring(start, end);
            } else if (line.contains("\\\"" + str + "\\\":\\\"")) {
                int start = line.indexOf("\\\"" + str + "\\\":\\\"") + 7 + str.length();
                int end = start + line.substring(start).indexOf("\\\"");
                ret = line.substring(start, end);
            }

        } catch (Exception e) {
            System.out.println(str);
        }
        return ret;
    }

    public Map getSubMap() {
        Map subMap = new HashMap();
        //时间
        String time = sub("logid");

        String day_id = "noday_id";
        String hour_id = "nohour_id";
        if (time.length() > 9) {
            day_id = time.substring(0, 8);
            hour_id = time.substring(8, 10);
        }
        //地区编码
        IpSearch finder = IpSearch.getInstance();
        String cityCode = sub("cityCode").equals("nocityCode") ? "000000" : sub("cityCode");
        String conty = "noconty";
        String latnCode = "nolatnCode";
        String regionCode = "noregionCode";
        String city = "nocity";
        String province = "noprovince";
        String requestIp = sub("requestip");
        String adress;
        if (!requestIp.equals("norequestip")) {
            adress = finder.Get(requestIp).toString().trim().replaceAll("\t", "");
            String[] splits = adress.split("\\|");

            if (splits.length == 11) {
                province = splits[2];
                city = splits[3];
                conty = splits[4];
            }
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

            regionCode = RegionUtil.getRegionCode(province);
            latnCode = LatnUtil.getRegionCode(city);
        }
        ;

        //imei
        String imei;
        if (!sub("imei").equals("noimei"))
            imei = sub("imei");
        else if (!sub("ashwid").equals("noashwid"))
            imei = sub("ashwid");
        else if (!sub("client_mark_id").equals("noclient_mark_id"))
            imei = sub("client_mark_id");
        else imei = "noimei";

        //channelno
        String channelno = matchNum(sub("requesttype"));
        //requesttype
        String requesttype = matchNum(sub("requesttype"));

        //subjectNum
        /*备注：位数均单指电话号码不含区号
         4	0开头，7位固定电话，8位固定电话，区号对照电话位数，见表；
         8	10位数非400、800开头不要	1
		 13	查有：1   查无：0
		 14	去重
		 15	比对 热度排序*/

        String sjNum = sub("subjectNum").replace("-", "");
        int sl = sjNum.length();
        String s4= sjNum.substring(0, 4);
        String s3= sjNum.substring(0, 3);
        if (sjNum.startsWith("+86") || sjNum.startsWith("086"))
            sjNum = sjNum.substring(3);
        else if (sjNum.startsWith("0086"))
            sjNum = sjNum.substring(4);
        //去掉特定意义符号后，过滤乱码，匹配规则
        if (!sjNum.matches("^\\d+$"))
            sjNum = "return";
        else if (sjNum.indexOf("1") != 0 && sjNum.indexOf("0") != 0 && sl > 10)
            sjNum = "return";
        else if (sl < 3 || sl == 4)
            sjNum = "return";
        else if (sl == 10) {
            if (s3 != "400" && s3 != "800")
                sjNum = "return";
            else if (s4 == "40005" || s4 == "4002" || s4 == "4003")
                sjNum = "return";
        } else if (sl == 3 && sjNum.indexOf("1") != 0)
            sjNum = "return";
        else if (sl == 5 || sl == 6) if (sjNum.startsWith("1") || sjNum.startsWith("9"))
            sjNum = "return";
        else if (sjNum.startsWith("0"))
            if (sl != 11 && sl != 12)
                sjNum = "return";


        subMap.put("regionCode", regionCode);
        subMap.put("latnCode", latnCode);
        subMap.put("day_id", day_id);
        subMap.put("hour_id", hour_id);
        subMap.put("cityCode", cityCode);
        subMap.put("imei", imei);
        subMap.put("channelno", channelno);
        subMap.put("requesttype", requesttype);
        subMap.put("sjNum",sjNum);
        return subMap;


    }

    public String matchNum(String str) {
        if (!str.equals("no" + str) && !str.matches("^\\d+$"))
            return "no" + str;
        else return str;
    }


    public BaseMr(String line) {
        this.line = line;

    }
}