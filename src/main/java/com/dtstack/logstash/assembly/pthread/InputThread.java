package com.dtstack.logstash.assembly.pthread;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.logstash.inputs.BaseInput;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:25:29
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class InputThread implements Runnable{
	
	private Logger logger = LoggerFactory.getLogger(InputThread.class);
	
	private BaseInput baseInput;
	
	private static ExecutorService inputExecutor;
	
	public InputThread(BaseInput baseInput){
		this.baseInput = baseInput;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		if(baseInput==null){
			logger.error("input plugin is not null");
			System.exit(1);
		}
		baseInput.emit();
	}
	
    /**
     * 
     * @param baseInputs
     * @return 
     */
	public static void initInputThread(List<BaseInput> baseInputs) {
		// TODO Auto-generated method stub
		if(inputExecutor==null)inputExecutor= Executors.newFixedThreadPool(baseInputs.size());
		for(BaseInput input:baseInputs){
			inputExecutor.submit(new InputThread(input));
		}
	}
}
