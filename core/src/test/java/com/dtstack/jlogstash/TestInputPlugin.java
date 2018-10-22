package com.dtstack.jlogstash;

import java.util.Map;

import com.dtstack.jlogstash.inputs.BaseInput;

public class TestInputPlugin extends BaseInput{

	public TestInputPlugin(Map config) {
		super(config);
		// TODO Auto-generated constructor stub
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
    
	/**
	 * 插件初始胡准备工作
	 */
	@Override
	public void prepare() {
		// TODO Auto-generated method stub
		
	}

	/**
	 * event事件处理工作
	 */
	@Override
	public void emit() {
		// TODO Auto-generated method stub
		
	}

	/**
	 * jvm 退出资源释放工作
	 */
	@Override
	public void release() {
		// TODO Auto-generated method stub
		
	}

}
