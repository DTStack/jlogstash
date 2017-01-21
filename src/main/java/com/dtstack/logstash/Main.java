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
package com.dtstack.logstash;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.logstash.assembly.AssemblyPipeline;
import com.dtstack.logstash.assembly.CmdLineParams;
import com.dtstack.logstash.exception.ExceptionUtil;
import com.dtstack.logstash.log.LogComponent;
import com.dtstack.logstash.log.LogbackComponent;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:24:26
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class Main {
	private static final Logger logger = LoggerFactory.getLogger(Main.class);
	
	private static AssemblyPipeline assemblyPipeline = new AssemblyPipeline();
	
	private static LogComponent logbackComponent = new LogbackComponent();
	
	public class Option {
		String flag, opt;

		public Option(String flag, String opt) {
			this.flag = flag;
			
			this.opt = opt;
		}
	}

	private static CommandLine parseArg(String[] args) throws ParseException {
        Options options = new Options();
		options.addOption("h", false, "usage help");
		options.addOption("help", false, "usage help");
		options.addOption("f", true, "configuration file");
		options.addOption("l", true, "log file");
		options.addOption("s", true, "disruptor wait stragety");
		options.addOption("w", true, "filter worker number");
		options.addOption("o", true, "output worker number");
		options.addOption("c", true, "output ringbuffer index");
		options.addOption("i", true, "input ringbuffer index");
		options.addOption("v", false, "print info log");
		options.addOption("vv", false, "print debug log");
		options.addOption("vvvv", false, "print trace log");
		CommandLineParser paraer = new BasicParser();
		CommandLine cmdLine = paraer.parse(options, args);
		if (cmdLine.hasOption("help") || cmdLine.hasOption("h")) {
			usage();
			System.exit(-1);
		}

		if (!cmdLine.hasOption("f")) {
			throw new ParseException("Required -f argument to specify config file");
		}
		return cmdLine;
	}

	/**
	 * print help information
	 */
	private static void usage() {
		StringBuilder helpInfo = new StringBuilder();
		helpInfo.append("-h").append("\t\t\thelp command").append("\n")
				.append("-help").append("\t\t\thelp command").append("\n")
				.append("-f").append("\t\t\trequired config, indicate config file").append("\n")
				.append("-l").append("\t\t\tlog file that store the output").append("\n")
				.append("-w").append("\t\t\tfilter worker numbers").append("\n")
				.append("-o").append("\t\t\toutput worker numbers").append("\n")
				.append("-i").append("\t\t\t input ringbuffer index").append("\n")
				.append("-c").append("\t\t\t output ringbuffer index").append("\n")
				.append("-v").append("\t\t\tprint info log").append("\n")
				.append("-vv").append("\t\t\tprint debug log").append("\n")
				.append("-vvvv").append("\t\t\tprint trace log").append("\n");
		System.out.println(helpInfo.toString());
	}


	public static void main(String[] args) {
		try {
			CommandLine cmdLine = parseArg(args);
            CmdLineParams.setLine(cmdLine);
			//logger config
            logbackComponent.setupLogger();
            //assembly pipeline
            assemblyPipeline.assemblyPipeline();
		} catch (Exception e) {
			logger.error("jlogstash start error:{}",ExceptionUtil.getErrorMessage(e));
			System.exit(-1);
		}
	}
}
