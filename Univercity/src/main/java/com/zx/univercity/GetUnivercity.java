package com.zx.univercity;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.zx.bean.Place;
import com.zx.bean.Univercity;

public class GetUnivercity {

	public static void main(String[] args) {
		List<Place> places = new ArrayList<Place>();
		Elements es = null;
		try {

			Document doc = Jsoup.parse(new URL("http://www.0755-0755.com/university/index.htm"), 6000);
			es = doc.select("div").get(1).select("table").get(1).select("td a");

		} catch (IOException e) {
			e.printStackTrace();
		}

		// 获得每个省份信息
		for (Element element : es) {
			String place_name = element.text();
			String place_url = element.attr("href");
			List<Univercity> uns = new ArrayList<Univercity>();

			// 将每个省份信息存储起来
			Place place = new Place(place_name, uns, place_url);

			// 获得每个大学信息
			Elements ees = null;
			try {
				ees = Jsoup.parse(new URL(place_url), 6000).select("div").get(2).select("td a");
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				continue;
			}
			for (Element element2 : ees) {
				String un_name = element2.text();
				String url = element2.attr("href");
				uns.add(new Univercity(un_name, url));
			}
			places.add(place);
		}

		BufferedWriter out = null;
		String path = Thread.currentThread().getContextClassLoader().getResource("univercity.txt").getPath();
		try {
			out = new BufferedWriter(new FileWriter(path));
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (Place pla : places) {
			for (Univercity un : pla.getUns()) {
				try {
					out.write(pla.getPlace_name() + "\t" + (un.getUn_name() + "," + un.getUrl()) + "\r\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		if(out != null) {
			try {
				out.flush();
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println(path);
		System.out.println("写入完成");
	}

}
