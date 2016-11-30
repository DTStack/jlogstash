package com.dtstack.logstash.assembly;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.logstash.assembly.pthread.FilterThread;
import com.dtstack.logstash.assembly.pthread.InputThread;
import com.dtstack.logstash.assembly.pthread.OutputThread;
import com.dtstack.logstash.assembly.qlist.InputQueueList;
import com.dtstack.logstash.assembly.qlist.OutPutQueueList;
import com.dtstack.logstash.configs.YamlConfig;
import com.dtstack.logstash.factory.InputFactory;
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
			
	private InputQueueList initInputQueueList;
	
	private OutPutQueueList initOutputQueueList;

	private List<BaseInput> baseInputs;
	
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
			initInputQueueList=InputQueueList.getInputQueueListInstance(CmdLineParams.getInputQueueNumber(cmdLine), CmdLineParams.getInputQueueSize(cmdLine));
			List<Map> inputs = (List<Map>) configs.get("inputs");
			if(inputs==null||inputs.size()==0){
				logger.error("input plugin is not empty");
				System.exit(1);
			}
			logger.debug("initOutputQueueList start ...");
			initOutputQueueList = OutPutQueueList.getOutPutQueueListInstance(CmdLineParams.getOutputQueueNumber(cmdLine), CmdLineParams.getOutputQueueSize(cmdLine));
		    List<Map> outputs = (List<Map>) configs.get("outputs");
			if(outputs==null||outputs.size()==0){
				logger.error("output plugin is not empty");
				System.exit(1);
			}
		    List<Map> filters = (List<Map>) configs.get("filters");
			logger.debug("init input plugin start ...");
			baseInputs =InputFactory.getBatchInstance(inputs, initInputQueueList);
			initInputQueueList.startElectionIdleQueue();
			if(CmdLineParams.isQueueSizeLog(cmdLine)){
				initInputQueueList.startLogQueueSize();
				initOutputQueueList.startLogQueueSize();	
			}
			logger.debug("input thread start ...");
			InputThread.initInputThread(baseInputs);
			logger.debug("filter thread start ...");
			FilterThread.initFilterThread(filters,CmdLineParams.getFilterWork(cmdLine),initInputQueueList,initOutputQueueList);
			logger.debug("output thread start ...");
			OutputThread.initOutPutThread(outputs,CmdLineParams.getOutputWork(cmdLine),initOutputQueueList,allBaseOutputs);
    		//add shutdownhook
    		ShutDownHook shutDownHook = new ShutDownHook(initInputQueueList,initOutputQueueList,baseInputs,allBaseOutputs);
    		shutDownHook.addShutDownHook();
		}catch(Exception t){
			logger.error("assemblyPipeline is error:{}", t.getCause());
			System.exit(1);
		}
	}
}
