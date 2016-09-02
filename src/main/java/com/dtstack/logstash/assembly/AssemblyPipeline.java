package com.dtstack.logstash.assembly;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.logstash.configs.YamlConfig;
import com.dtstack.logstash.factory.FilterFactory;
import com.dtstack.logstash.factory.InputFactory;
import com.dtstack.logstash.factory.OutputFactory;
import com.dtstack.logstash.filters.BaseFilter;
import com.dtstack.logstash.inputs.BaseInput;
import com.dtstack.logstash.outputs.BaseOutput;
import com.dtstack.logstash.property.SystemProperty;
import com.dtstack.logstash.utils.Machine;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:25:11
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class AssemblyPipeline {
	
	private static Logger logger = LoggerFactory.getLogger(AssemblyPipeline.class);
	
	private static ExecutorService filterOutputExecutor =null;
	
	private static ExecutorService inputExecutor =null;
	
	private InputQueueList initInputQueueList =null;
	
	private List<BaseInput> baseInputs =null;
	
	/**
	 * 组装管道
	 * @param cmdLine
	 * @return 
	 * @throws IOException
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public InputQueueList assemblyPipeline(CommandLine cmdLine) throws IOException{
		try{
			logger.debug("load config start ...");
			Map configs = new YamlConfig().parse(cmdLine.getOptionValue("f"));
			logger.debug(configs.toString());
			logger.debug("initInputQueueList start ...");
			initInputQueueList=initInputQueueList(cmdLine);
			List<Map> inputs = (List<Map>) configs.get("inputs");
			if(inputs==null||inputs.size()==0){
				logger.error("input plugin is not empty");
				System.exit(1);
			}
			
		    List<Map> outputs = (List<Map>) configs.get("outputs");
			if(outputs==null||outputs.size()==0){
				logger.error("output plugin is not empty");
				System.exit(1);
			}
		    List<Map> filters = (List<Map>) configs.get("filters");
			logger.debug("init input plugin start ...");
			baseInputs =InputFactory.getBatchInstance(inputs, initInputQueueList);
			initInputQueueList.startElectionIdleQueue();
			if(isInputQueueSizeLog(cmdLine))initInputQueueList.startLogQueueSize();
			logger.debug("input thread start ...");
			initInputPutThread(baseInputs);
			logger.debug("FilterAndOutput thread start ...");
		    initFilterAndOutputThread(outputs,filters,initInputQueueList.getQueueList(),getOutBatchSize(cmdLine));
		}catch(Exception t){
			logger.error("assemblyPipeline is error", t);
			System.exit(1);
		}
        return initInputQueueList;
	}


	protected InputQueueList initInputQueueList(CommandLine cmdLine){
		int filterWorks = getFilterWork(cmdLine);
        int queueSize = getInputQueueSize(cmdLine);
        InputQueueList queueList = new InputQueueList();
        List<LinkedBlockingQueue<Map<String,Object>>> list =queueList.getQueueList();
        for(int i=0;i<filterWorks;i++){
        	list.add(new LinkedBlockingQueue<Map<String,Object>>(queueSize));
        }
		return queueList;
	}
	
	
	protected void initInputPutThread(List<BaseInput> baseInputs) {
		// TODO Auto-generated method stub
		inputExecutor= Executors.newFixedThreadPool(baseInputs.size());
		for(BaseInput input:baseInputs){
			inputExecutor.submit(new InputThread(input));
		}
	}
	
	protected void initFilterAndOutputThread(List<Map> outputs, List<Map> filters, List<LinkedBlockingQueue<Map<String,Object>>> queues,int batchSize) throws Exception{
		filterOutputExecutor= Executors.newFixedThreadPool(queues.size());
		for(LinkedBlockingQueue<Map<String,Object>> queue:queues){
			List<BaseOutput> baseOutputs = OutputFactory.getBatchInstance(outputs);		
			List<BaseFilter> baseFilters = FilterFactory.getBatchInstance(filters);	
			filterOutputExecutor.submit(new FilterAndOutputThread(queue,baseFilters,baseOutputs,batchSize));
		}
	}
	
	/**
	 * 获取filter线程数
	 * @param line
	 * @return
	 */
	protected static int getFilterWork(CommandLine line){
		String number =line.getOptionValue("w");
        int works =StringUtils.isNotBlank(number)?Integer.parseInt(number):Machine.availableProcessors();	
        logger.warn("getFilterWork--->"+works); 	
        return works;
	}
	
	/**
	 *获取queue size的大小
	 * @param line
	 * @return
	 */
	protected static int getInputQueueSize(CommandLine line){
		String number =line.getOptionValue("q");
        return 	StringUtils.isNotBlank(number)?Integer.parseInt(number):Integer.parseInt(SystemProperty.getSystemProperty("inputQueueSize"));	
	}
	
	/**
	 * 获取batch size的大小
	 * @param line
	 * @return
	 */
	protected static int getOutBatchSize(CommandLine line){
		String number =line.getOptionValue("b");
        return 	StringUtils.isNotBlank(number)?Integer.parseInt(number):Integer.parseInt(SystemProperty.getSystemProperty("batchSize"));	
	}
	
	
	/**
	 * 是否开启InputQueueSize log日志标准输出
	 * @param line
	 * @return
	 */
	protected static boolean isInputQueueSizeLog(CommandLine line){
		return line.hasOption("t");
	}


	public List<BaseInput> getBaseInputs() {
		return baseInputs;
	}
}
