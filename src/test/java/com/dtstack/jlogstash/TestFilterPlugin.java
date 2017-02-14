package com.dtstack.jlogstash;

import java.util.Map;

import com.dtstack.jlogstash.filters.BaseFilter;

public class TestFilterPlugin extends BaseFilter{

	public TestFilterPlugin(Map config) {
		super(config);
		// TODO Auto-generated constructor stub
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 插件初始化准备工作
	 */
	@Override
	public void prepare() {
		// TODO Auto-generated method stub
		
	}

	/***
	 * event 事件过滤工作
	 */
	@Override
	protected Map filter(Map event) {
		// TODO Auto-generated method stub
		return null;
	}

}
