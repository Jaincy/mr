package com.yp114.omc.grasp;

import com.yp114.omc.apponofflog.BaseMr;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.http.client.methods.HttpGet;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Test;


import java.io.IOException;

/**
 * Created by Jain on 2016/5/19.
 */
public class Huawei {

    @Test
    public void jiexi() throws IOException {
        String url="http://180.153.50.28:9080/hy114/ClassGet?tel=02488121237" ;

        Document doc = Jsoup.connect(url).get();
        String body = doc.body().toString();
        System.out.print(body);
        String st=StringEscapeUtils.unescapeHtml((body));
        System.out.println(st);
        BaseMr bm=new BaseMr(st);
        String classcode1=bm.sub("classcode1");
        String classcode2=bm.sub("classcode2");
        String classname1=bm.sub("classname1");
        String classname2=bm.sub("classname2");
        System.out.println(classcode1+"\t"+classcode2);



    }
    public static void main(String[] args) throws IOException {
        Huawei h=new Huawei();
       // h.jiexi();
        String s=new String("sdf");
        h.test(h);


    }
    public static void test(Object o){
        System.out.println("objedct");
    }
    public static void test(String s){
        System.out.println("string");

    }
    @Test
    public  void test(){
        String s="sdf5sf46s4f654df923e";
        System.out.println(s.replaceAll("[^\\d]",""));


    }





}
