package com.yp114.omc.grasp;
import com.yp114.omc.apponofflog.BaseMr;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Test;

/**
 * Created by Jain on 2016/5/25.
 */
public final class HuaweiUdf extends UDF  {





        @Test
        public String evaluate(String str1) {
            char ch=0x01;

            try {
                //http://180.153.50.195:7080/hy114/ClassGet?tel=02488121237

                String url="http://180.153.50.195:7080/hy114/ClassGet?tel="+str1;

                //jsoup
                Document doc = Jsoup.connect(url).get();
                String body = doc.body().toString();
                String st= StringEscapeUtils.unescapeHtml((body));
                //System.out.println(st);
                BaseMr bm=new BaseMr(st);

                String classcode1=bm.sub("classcode1");
                String classcode2=bm.sub("classcode2");
                String classname1=bm.sub("classname1");
                String classname2=bm.sub("classname2");
                //System.out.println(classcode1+ch+classcode2+ch+classname1+ch+classname2);
                String str2=classcode1+ch+classcode2+ch+classname1+ch+classname2;

                return  str2;

            } catch (Exception e) {

                return "noclasscode1"+ch+"noclasscode2"+ch+"noclassname1"+ch+"noclassname2";

            }

        }

    }


