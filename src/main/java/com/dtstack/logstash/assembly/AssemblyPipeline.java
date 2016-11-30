package com.dtstack.logstash.assembly;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.logstash.assembly.pluginThread.FilterThread;
import com.dtstack.logstash.assembly.pluginThread.InputThread;
import com.dtstack.logstash.assembly.pluginThread.OutputThread;
import com.dtstack.logstash.assembly.queueList.InputQueueList;
import com.dtstack.logstash.assembly.queueList.OutPutQueueList;
import com.dtstack.logstash.configs.YamlConfig;
import com.dtstack.logstash.factory.FilterFactory;
import com.dtstack.logstash.factory.InputFactory;
import com.dtstack.logstash.factory.OutputFactory;
import com.dtstack.logstash.filters.BaseFilter;
import com.dtstack.logstash.inputs.BaseInput;
import com.dtstack.logstash.outputs.BaseOutput;
import com.google.common.collect.Lists;

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
	
	private static ExecutorService filterExecutor =null;
	
	private static ExecutorService outputExecutor =null;
	
	private static ExecutorService inputExecutor =null;
	
	private InputQueueList initInputQueueList =null;
	
	private OutPutQueueList initOutputQueueList =null;

	private List<BaseInput> baseInputs =null;
	
	private List<BaseOutput> allBaseOutputs = Lists.newCopyOnWriteArrayList();
	
	/**
	 * 组装管道
	 * @param cmdLine
	 * @return 
	 * @throws IOException
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void assemblyPipeline(CommandLine cmdLine) throws IOException{
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
			logger.debug("initOutputQueueList start ...");
			initOutputQueueList = initOutputQueueList(cmdLine);
		    List<Map> outputs = (List<Map>) configs.get("outputs");
			if(outputs==null||outputs.size()==0){
				logger.error("output plugin is not empty");
				System.exit(1);
			}
		    List<Map> filters = (List<Map>) configs.get("filters");
			logger.debug("init input plugin start ...");
			baseInputs =InputFactory.getBatchInstance(inputs, initInputQueueList);
			initInputQueueList.startElectionIdleQueue();
			if(CmdLineParams.isQueueSizeLog(cmdLine))initInputQueueList.startLogQueueSize();
			logger.debug("input thread start ...");
			initInputThread(baseInputs);
			logger.debug("filter thread start ...");
			initFilterThread(filters,CmdLineParams.getFilterWork(cmdLine));
			logger.debug("output thread start ...");
			initOutPutThread(outputs,CmdLineParams.getOutputWork(cmdLine));
		}catch(Exception t){
			logger.error("assemblyPipeline is error:{}", t.getCause());
			System.exit(1);
		}
	}

 /**
  *   
  * @param cmdLine
  * @return
  */
	protected InputQueueList initInputQueueList(CommandLine cmdLine){
		int inputQueue = CmdLineParams.getInputQueueNumber(cmdLine);
        int queueSize = CmdLineParams.getInputQueueSize(cmdLine);
        InputQueueList queueList = new InputQueueList();
        List<LinkedBlockingQueue<Map<String,Object>>> list =queueList.getQueueList();
        for(int i=0;i<inputQueue;i++){
        	list.add(new LinkedBlockingQueue<Map<String,Object>>(queueSize));
        }
		return queueList;
	}
	
	
    /**
     * 
     * @param cmdLine
     * @return
     */
	protected OutPutQueueList initOutputQueueList(CommandLine cmdLine){
		int outPutQueue = CmdLineParams.getOutputQueueNumber(cmdLine);
        int queueSize = CmdLineParams.getOutputQueueSize(cmdLine);
        OutPutQueueList queueList = new OutPutQueueList();
        List<LinkedBlockingQueue<Map<String,Object>>> list =queueList.getQueueList();
        for(int i=0;i<outPutQueue;i++){
        	list.add(new LinkedBlockingQueue<Map<String,Object>>(queueSize));
        }
		return queueList;
	}
	
	
    /**
     * 
     * @param baseInputs
     */
	protected void initInputThread(List<BaseInput> baseInputs) {
		// TODO Auto-generated method stub
		inputExecutor= Executors.newFixedThreadPool(baseInputs.size());
		for(BaseInput input:baseInputs){
			inputExecutor.submit(new InputThread(input));
		}
	}
	
	
	/**
	 * 
	 * @param filters
	 * @param works
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	protected void initFilterThread(List<Map> filters,int works) throws Exception{
		FilterThread.setInPutQueueList(initInputQueueList);
		FilterThread.setOutPutQueueList(initOutputQueueList);
		filterExecutor= Executors.newFixedThreadPool(works);
		for(int i=0;i<works;i++){
			List<BaseFilter> baseFilters = FilterFactory.getBatchInstance(filters);	
			filterExecutor.submit(new FilterThread(baseFilters));
		}
	}
	
	
	/**
	 * 
	 * @param outputs
	 * @param works
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	protected void initOutPutThread(List<Map> outputs,int works) throws Exception{
		OutputThread.setOutPutQueueList(initOutputQueueList);
		outputExecutor= Executors.newFixedThreadPool(works);
		for(int i=0;i<works;i++){
			List<BaseOutput> baseOutputs = OutputFactory.getBatchInstance(outputs);
			allBaseOutputs.addAll(baseOutputs);
			outputExecutor.submit(new OutputThread(baseOutputs));
		}
	}

	public List<BaseInput> getBaseInputs() {
		return this.baseInputs;
	}


	public InputQueueList getInitInputQueueList() {
		return initInputQueueList;
	}

	public OutPutQueueList getInitOutputQueueList() {
		return initOutputQueueList;
	}


	public List<BaseOutput> getAllBaseOutputs() {
		return allBaseOutputs;
	}

}
