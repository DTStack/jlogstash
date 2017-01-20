/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.logstash.assembly;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.logstash.assembly.disruptor.JDisruptor;
import com.dtstack.logstash.assembly.pthread.FilterHandler;
import com.dtstack.logstash.assembly.pthread.InputThread;
import com.dtstack.logstash.assembly.pthread.OutputHandler;
import com.dtstack.logstash.configs.YamlConfig;
import com.dtstack.logstash.exception.LogstashException;
import com.dtstack.logstash.factory.InputFactory;
import com.dtstack.logstash.factory.InstanceFactory;
import com.dtstack.logstash.inputs.BaseInput;
import com.dtstack.logstash.outputs.BaseOutput;
import com.google.common.collect.Lists;
import  com.dtstack.logstash.classloader.JarClassLoader;

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
				
	private List<List<BaseOutput>> allBaseOutputs = Lists.newCopyOnWriteArrayList();
	
	private JarClassLoader JarClassLoader = new JarClassLoader();
	

	/**
	 * 组装管道
	 * @param cmdLine
	 * @return 
	 * @throws IOException
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void assemblyPipeline(CommandLine cmdLine) throws Exception{
			logger.debug("load config start ...");
			Map configs = new YamlConfig().parse(cmdLine.getOptionValue("f"));
			logger.debug(configs.toString());
			logger.debug("load plugin...");
			InstanceFactory.setClassCloaders(JarClassLoader.loadJar());
			List<Map> inputs = (List<Map>) configs.get("inputs");
			if(inputs==null||inputs.size()==0){
				throw new LogstashException("input plugin is not empty");
			}
			List<Map> outputs = (List<Map>) configs.get("outputs");
			if(outputs==null||outputs.size()==0){
				throw new LogstashException("output plugin is not empty");
			}
		    List<Map> filters = (List<Map>) configs.get("filters");
		    int filterWorks = CmdLineParams.getFilterWork();
		    JDisruptor inputToFilterDisruptor = new JDisruptor(FilterHandler.getArrayHandlerInstance(filters,filterWorks),CmdLineParams.getFilterRingBuffer(),CmdLineParams.getWaitStrategy(),filterWorks);
		    int outputWorks = CmdLineParams.getOutputWork();
		    JDisruptor filterToOutputDisruptor = new JDisruptor(OutputHandler.getArrayHandlerInstance(outputs,outputWorks,allBaseOutputs),CmdLineParams.getFilterRingBuffer(),CmdLineParams.getWaitStrategy(),outputWorks);
		    List<BaseInput> baseInputs =InputFactory.getBatchInstance(inputs,inputToFilterDisruptor);
			InputThread.initInputThread(baseInputs);
    		//add shutdownhook
    		ShutDownHook shutDownHook = new ShutDownHook(inputToFilterDisruptor,filterToOutputDisruptor,baseInputs,allBaseOutputs);
    		shutDownHook.addShutDownHook();
	}
}