package com.dahua.tolls;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.Map;

public class SNTools {

//    public static void main(String[] args) throws Exception {
//
//
//        SNTools snTools = new SNTools();
//        snTools.getBusiness("39.91037960975796,116.47178829650882");
//        //116.3161200000,39.9850750000
//    }

    public static String getBusiness(String latAndLong) throws Exception,
            NoSuchAlgorithmException {
        SNTools snCal = new SNTools();

// 计算sn跟参数对出现顺序有关，get请求请使用LinkedHashMap保存<key,value>，该方法根据key的插入顺序排序；post请使用TreeMap保存<key,value>，该方法会自动将key按照字母a-z顺序排序。所以get请求可自定义参数顺序（sn参数必须在最后）发送请求，但是post请求必须按照字母a-z顺序填充body（sn参数必须在最后）。以get请求为例：http://api.map.baidu.com/geocoder/v2/?address=百度大厦&output=json&ak=yourak，paramsMap中先放入address，再放output，然后放ak，放入顺序必须跟get请求中对应参数的出现顺序保持一致。
// 请求参数。
        Map paramsMap = new LinkedHashMap<String, String>();
        paramsMap.put("callback", "renderReverse");
        paramsMap.put("output", "json");
        paramsMap.put("location", latAndLong);
        paramsMap.put("pois", "1");
        paramsMap.put("ak", "tdNzn1bXNr7BPR2LD0m65hV4S5W0rDvy");
        String business = "";

        // 调用下面的toQueryString方法，对LinkedHashMap内所有value作utf8编码，拼接返回结果address=%E7%99%BE%E5%BA%A6%E5%A4%A7%E5%8E%A6&output=json&ak=yourak
        String paramsStr = snCal.toQueryString(paramsMap);
        System.out.println(paramsStr);

        // 对paramsStr前面拼接上/geocoder/v2/?，后面直接拼接yoursk得到/geocoder/v2/?address=%E7%99%BE%E5%BA%A6%E5%A4%A7%E5%8E%A6&output=json&ak=yourakyoursk
        String wholeStr = new String("/reverse_geocoding/v3/?" + paramsStr + "rcSoDCcDyHQhWKX0G9Y7rBLAFdE7ydis");

        // 对上面wholeStr再作utf8编码
        String tempStr = URLEncoder.encode(wholeStr, "UTF-8");

        // 调用下面的MD5方法得到最后的sn签名7de5a22212ffaa9e326444c75a58f9a0
        System.out.println(snCal.MD5(tempStr));
        String sn = snCal.MD5(tempStr); // 最终的sn码.
        // 模拟get请求。获得百度地图的商圈返回值。


        HttpClient httpClient = new HttpClient();// 模拟浏览器请求。  请求到达上限。
        GetMethod getMethod = new GetMethod("http://api.map.baidu.com/reverse_geocoding/v3/?" + paramsStr + "&sn=" + sn);
        System.out.println("http://api.map.baidu.com/reverse_geocoding/v3/?" + paramsStr + "&sn=" + sn);


        int code = httpClient.executeMethod(getMethod);
        // 如果返回值是200,证明成功。
        // 获取的response返回值结果。
        String responseBody = getMethod.getResponseBodyAsString();
        System.out.println(responseBody);
        // 关闭连接。
        getMethod.releaseConnection();
        // 解析这个json。用fastjosn 阿里巴巴提供的。
        if (responseBody.startsWith("renderReverse&&renderReverse(")) {
            String replace = responseBody.replace("renderReverse&&renderReverse(", "");
            // 截取字符串。
            String substring = replace.substring(0, replace.lastIndexOf(")"));
            // 将josn字符串转换成jsonObject
            JSONObject jsonObject = JSON.parseObject(substring);
            JSONObject result = jsonObject.getJSONObject("result");
            business = result.getString("business");
            // 最终结果：
            System.out.println(business);
            // 有可能business是空的。
            if (business == null || StringUtils.isEmpty(business)) {
                JSONArray pois = jsonObject.getJSONArray("pois");
                if (pois != null && pois.size() > 0) {
                    business = pois.getJSONObject(0).getString("tag");
                }
            }
        }
        return business;
    }

    public String toQueryString(Map<?, ?> data)
            throws UnsupportedEncodingException {
        StringBuffer queryString = new StringBuffer();
        for (Map.Entry<?, ?> pair : data.entrySet()) {
            queryString.append(pair.getKey() + "=");
            queryString.append(URLEncoder.encode((String) pair.getValue(),
                    "UTF-8") + "&");
        }
        if (queryString.length() > 0) {
            queryString.deleteCharAt(queryString.length() - 1);
        }
        return queryString.toString();
    }

    // 来自stackoverflow的MD5计算方法，调用了MessageDigest库函数，并把byte数组结果转换成16进制
    public String MD5(String md5) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest
                    .getInstance("MD5");
            byte[] array = md.digest(md5.getBytes());
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < array.length; ++i) {
                sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100)
                        .substring(1, 3));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
        }
        return null;
    }
}
