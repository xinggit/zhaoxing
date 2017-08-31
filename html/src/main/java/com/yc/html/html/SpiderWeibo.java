package com.yc.html.html;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.Logger;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;


public class SpiderWeibo {

	public final static Logger LOG = Logger.getLogger(SpiderWeibo.class);
	
	public static void main(String[] args) {
		URL url = null;
		try {
			url = new URL("https://translate.googleapis.com/translate_a/t?anno=3&client=te_lib&format=html&v=1.0&key=AIzaSyBOti4mM-6x9WDnZIjIeyEU21OpBXqWBgw&logld=vTE_20170814_01&sl=zh-CN&tl=zh-CN&tc=1&tk=909924.769303&mode=1");
		} catch (MalformedURLException e) {
			LOG.error("URL解析错误", e);
		} 
		
		LOG.info("URL获取成功");
		Connection con = null;
		con = Jsoup.connect(url.toString());
		LOG.info("网址连接成功");
		Document html = null;
		try {
			html = con
//						.data("latitude","26.88021")
//						.data("longitude", "112.68484")
//						.header("cache-control","max-age=0")
//						.header("referer", "https://h5.ele.me/login/")
//						.header("upgrade-insecure-requests", "1")
//						.header("authority", "www.ele.me")
//						.header("scheme", "HTTPS")
//						.header("path", "/place/wsb0ujqse46?latitude=26.88021&longitude=112.68484")
//						.header("accept-encoding", "gzip, deflate, sdch, br")
					.header("content-type", "text/*")
					.header("Accept", "*/*")
					.header("content-length", "4754")
					.header("referer", "https://www.ele.me/place/wsb0ujqse46?latitude=26.88021&longitude=112.68484")
						.header("Cookie", "ubt_ssid=gnm9z2xevt5mvovmr9uwzob1eh43teyd_2017-08-24; _utrace=7cbad3aef6dad2178dae02cadede559b_2017-08-24; pageReferrInSession=https%3A//www.ele.me/place/wsb0ujqse46%3Flatitude%3D26.88021%26longitude%3D112.68484; firstEnterUrlInSession=https%3A//www.ele.me/shop/156049703; perf_ssid=bkm5c2ujlecuk57jg6qz16zobetclvnx_2017-08-24; eleme__ele_me=3a98a7a3b24e22516202158f56da683e%3A6f03313181befe8d6232306d2da3eded6772eef2; SID=hqCVSLi6aLvd5jNIAdkGOpswalROu5nAqiSA; USERID=8707546; track_id=1503542096%7C08a1a65291ab73d741916c0cb5021f97514966469b9627293a%7C6c12d11904eba0016bee56071a9b591e")
//						.header("If-Modified-Since", "Tue, 22 Aug 2017 07:48:10 GMT")
//						.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.81 Safari/537.36")
						.post();
			
		} catch (IOException e) {
			LOG.error("get请求失败", e);
		}
		LOG.info("get请求成功");	
		System.out.println(html.toString());
		
	}

}