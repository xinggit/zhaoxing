package com.zx.bean;

public class Univercity {
	private String Un_name;
	private String url;

	public Univercity(String un_name, String url) {
		Un_name = un_name;
		this.url = url;
	}

	public Univercity() {
	}
	
	public final String getUrl() {
		return url;
	}

	public final void setUrl(String url) {
		this.url = url;
	}

	public final String getUn_name() {
		return Un_name;
	}

	public final void setUn_name(String un_name) {
		Un_name = un_name;
	}

	@Override
	public String toString() {
		return  Un_name + url + "\n";
	}
	
	
	
}
