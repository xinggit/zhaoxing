package com.yc.html.bean;

public class WeiBoGson {
	private String code;
	private String msg;
	private String data;
	
	public WeiBoGson() {
	}

	public WeiBoGson(String code, String msg, String data) {
		super();
		this.code = code;
		this.msg = msg;
		this.data = data;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

}
