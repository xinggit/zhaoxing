package com.zx.bean;

import java.util.List;

public class Place {
	private String place_name;
	private List<Univercity> uns;
	private String place_url;

	public Place() {
	}

	public Place(String place_name, List<Univercity> uns) {
		this.place_name = place_name;
		this.uns = uns;
	}

	public Place(String place_name, List<Univercity> uns, String place_url) {
		super();
		this.place_name = place_name;
		this.uns = uns;
		this.place_url = place_url;
	}

	public final String getPlace_url() {
		return place_url;
	}

	public final void setPlace_url(String place_url) {
		this.place_url = place_url;
	}

	public final String getPlace_name() {
		return place_name;
	}

	public final void setPlace_name(String place_name) {
		this.place_name = place_name;
	}

	public final List<Univercity> getUns() {
		return uns;
	}

	public final void setUns(List<Univercity> uns) {
		this.uns = uns;
	}

	@Override
	public String toString() {
		return place_name + place_url + "\n" + uns;
	}

}
