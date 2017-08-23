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
			url = new URL("http://weibo.com/");
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
						.cookie("cross_origin_proto", "deleted")
						.cookie("expires", "Thu, 01-Jan-1970 00:00:01 GMT")
						.cookie("path", "/")
						.cookie("domain", ".weibo.com")
						.data("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8")
						.data("Cookie", "SINAGLOBAL=7145586631874.236.1500684958059; UM_distinctid=15de903bb2c34c-08e98b49d3f7f6-474a0521-144000-15de903bb2d72e; login_sid_t=d8fdd6ca3817788d67b6ed5a67d93ebf; TC-Ugrow-G0=968b70b7bcdc28ac97c8130dd353b55e; TC-V5-G0=52dad2141fc02c292fc30606953e43ef; _s_tentry=-; Apache=3478571267542.9893.1503448586447; ULV=1503448586462:13:11:4:3478571267542.9893.1503448586447:1503405779226; YF-Page-G0=8eed071541083194c6ced89d2e32c24a; YF-Ugrow-G0=56862bac2f6bf97368b95873bc687eef; YF-V5-G0=a53c7b4a43414d07adb73f0238a7972e; WBtopGlobal_register_version=016bacd40fc16287; appkey=; SUHB=0vJISWjQjPcsRt; wb_cmtLike_5192249270=1; SUB=_2AkMuwFkldcPxrAZQkfoXyGLga41H-jydFTDTAn7uJhMyAxgv7m5WqSWQnhK80_-KMLUfwSRUe8spT0XgDA..; SUBP=0033WrSXqPxfM72wWs9jqgMF55529P9D9WWaj9FafIwuTA79e6ANm3wX5JpVF02feKM0ehBRehzc; UOR=www.csdn.net,widget.weibo.com,login.sina.com.cn; WBStorage=0c663978e8e51f06|undefined")
						.data("If-Modified-Since", "Wed, 23 Aug 2017 01:46:54 GMT")
						.data("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36")
						.get();
		} catch (IOException e) {
			LOG.error("get请求失败", e);
		}
		LOG.info("get请求成功");	
			
		System.out.println(html.toString());
		
	}

}