package com.dtstack.jlogstash;

import java.util.Map;

import com.dtstack.jlogstash.outputs.BaseOutput;


/**
 * output plugin 样列
 * @author sishu.yss
 *
 */
public class TestOutputPlugin extends BaseOutput{

	public TestOutputPlugin(Map config) {
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

	/**
	 * 事件逻辑处理
	 */
	@Override
	protected void emit(Map event) {
		// TODO Auto-generated method stub
		
	}
	
	/**
	 * jvm 退出时插件所有释放的资源
	 */
	@Override
	public void release(){}
	
	/**
	 * 数据失败时候处理的逻辑
	 */
	@Override
	public void sendFailedMsg(Object msg){}

}
