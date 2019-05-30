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
package com.dtstack.jlogstash.classloader;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import com.dtstack.jlogstash.assembly.CmdLineParams;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.jlogstash.exception.ExceptionUtil;
import com.dtstack.jlogstash.exception.LogstashException;
import com.google.common.collect.Maps;

/**
 *
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年12月16日 下午15:26:07
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class JARClassLoaderFactory {

    private static Logger logger = LoggerFactory.getLogger(JARClassLoaderFactory.class);

    private static String pluginDir = StringUtils.isNotBlank(CmdLineParams.getPluginPath())?CmdLineParams.getPluginPath():System.getProperty("user.dir");

    private Map<String,URL[]> jarUrls = null;

    private static JARClassLoaderFactory jarClassLoaderInstance = null;

    private static Map<String, ClassLoader> pluginClassLoader = Maps.newConcurrentMap();

    private JARClassLoaderFactory(){
        if(jarUrls==null){
            jarUrls = getClassLoadJarUrls();
        }
    }

    public static JARClassLoaderFactory getInstance(){
        if(jarClassLoaderInstance==null){
            synchronized (JARClassLoaderFactory.class){
                if(jarClassLoaderInstance == null){
                    jarClassLoaderInstance = new JARClassLoaderFactory();
                }
            }
        }
        return jarClassLoaderInstance;
    }



    public ClassLoader getClassLoaderByPluginName(String name){
        ClassLoader classLoader = pluginClassLoader.computeIfAbsent(name, k -> {
            URL[] urls =  jarUrls.get(name);
            if(urls==null || urls.length==0){
                logger.warn("{}:load by AppclassLoader",name);
                return this.getClass().getClassLoader();
            }
            return new JARClassLoader(urls, this.getClass().getClassLoader());
        });
        return classLoader;
    }

    private Map<String,URL[]> getClassLoadJarUrls(){
        Map<String,URL[]>  result  = Maps.newConcurrentMap();
        try{
            logger.warn("userDir:{}",pluginDir);
            String input = String.format("%s/plugin/input", pluginDir);
            File finput = new File(input);
            if(!finput.exists()){
                throw new LogstashException(String.format("%s direcotry not found", input));
            }

            String filter = String.format("%s/plugin/filter", pluginDir);
            File ffilter = new File(filter);
            if(!ffilter.exists()){
                throw new LogstashException(String.format("%s direcotry not found", filter));
            }

            String output = String.format("%s/plugin/output", pluginDir);
            File foutput = new File(output);
            if(!foutput.exists()){
                throw new LogstashException(String.format("%s direcotry not found", output));
            }
            Map<String,URL[]>  inputs = getClassLoadJarUrls(finput);
            result.putAll(inputs);
            Map<String,URL[]>  filters = getClassLoadJarUrls(ffilter);
            result.putAll(filters);
            Map<String,URL[]>  outputs = getClassLoadJarUrls(foutput);
            result.putAll(outputs);
            logger.warn("getClassLoadJarUrls:{}",result);
        }catch(Exception e){
            logger.error("getClassLoadJarUrls error:{}",ExceptionUtil.getErrorMessage(e));
        }
        return result;
    }

    private Map<String,URL[]> getClassLoadJarUrls(File dir) throws MalformedURLException, IOException{
        String dirName = dir.getName();
        Map<String,URL[]> jurls = Maps.newConcurrentMap();
        File[] files = dir.listFiles();
        if (files!=null&&files.length>0){
            for(File f:files){
                String jarName = f.getName();
                if(f.isFile()&&jarName.endsWith(".jar")){
                    jarName = jarName.split("-")[0].toLowerCase();
                    String[] jns = jarName.split("\\.");
                    jurls.put(String.format("%s:%s",dirName,jns.length==0?jarName:jns[jns.length-1]), new URL[]{f.toURI().toURL()});
                }
            }
        }
        return jurls;
    }
}