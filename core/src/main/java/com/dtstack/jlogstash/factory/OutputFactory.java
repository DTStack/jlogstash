/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.jlogstash.factory;

import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.dtstack.jlogstash.callback.ClassLoaderCallBackMethod;
import com.dtstack.jlogstash.outputs.BaseOutput;
import com.dtstack.jlogstash.outputs.IBaseOutput;
import com.dtstack.jlogstash.outputs.OutputProxy;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:26:39
 * Company: www.dtstack.com
 *
 * @author sishu.yss
 */
public class OutputFactory extends InstanceFactory {

    private final static String PLUGINTYPE = "output";

    @SuppressWarnings("rawtypes")
    private static IBaseOutput getInstance(String outputType, Map outputConfig) throws Exception {
        ClassLoader classLoader = getClassLoader(outputType, PLUGINTYPE);
        BaseOutput outputInstance = ClassLoaderCallBackMethod.callbackAndReset(() -> {
            Class<?> outputClass = classLoader.loadClass(getClassName(outputType, PLUGINTYPE));
            configInstance(outputClass, outputConfig);
            //设置static field
            Constructor<?> ctor = outputClass.getConstructor(Map.class);
            return (BaseOutput) ctor.newInstance(outputConfig);
        }, classLoader, true);
        //设置非static field
        configInstance(outputInstance, outputConfig);
        IBaseOutput baseOutput = new OutputProxy(outputInstance);
        baseOutput.prepare();
        return baseOutput;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static List<IBaseOutput> getBatchInstance(List<Map> outputs) throws Exception {
        if (outputs == null || outputs.size() == 0) {
            return null;
        }
        List<IBaseOutput> baseOutputs = Lists.newArrayList();
        for (int i = 0; i < outputs.size(); i++) {
            Iterator<Entry<String, Map>> outputIT = outputs.get(i).entrySet().iterator();
            while (outputIT.hasNext()) {
                Map.Entry<String, Map> outputEntry = outputIT.next();
                String outputType = outputEntry.getKey();
                Map outputConfig = outputEntry.getValue();
                if (outputConfig == null) {
                    outputConfig = Maps.newLinkedHashMap();
                }
                IBaseOutput baseOutput = getInstance(outputType, outputConfig);
                baseOutputs.add(baseOutput);
            }
        }
        return baseOutputs;
    }
}
