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

package com.dtstack.jlogstash.outputs;

import com.dtstack.jlogstash.callback.ClassLoaderCallBackMethod;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/5/29
 */
public class OutputProxy implements IBaseOutput {

    private IBaseOutput baseOutput;

    public OutputProxy(IBaseOutput baseOutput) {
        this.baseOutput = baseOutput;
    }

    @Override
    public void prepare() {
        try {
            ClassLoaderCallBackMethod.callbackAndReset(() -> {
                baseOutput.prepare();
                return null;
            }, baseOutput.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void process(Map event) {
        try {
            ClassLoaderCallBackMethod.callbackAndReset(() -> {
                baseOutput.process(event);
                return null;
            }, baseOutput.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void release() {
        try {
            ClassLoaderCallBackMethod.callbackAndReset(() -> {
                baseOutput.release();
                return null;
            }, baseOutput.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public AtomicInteger getAto() {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> {
                return baseOutput.getAto();
            }, baseOutput.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isConsistency() {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> {
                return baseOutput.isConsistency();
            }, baseOutput.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean dealFailedMsg() {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> {
                return baseOutput.dealFailedMsg();
            }, baseOutput.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addFailedMsg(Object msg) {
        try {
            ClassLoaderCallBackMethod.callbackAndReset(() -> {
                baseOutput.addFailedMsg(msg);
                return null;
            }, baseOutput.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sendFailedMsg(Object msg) {
        try {
            ClassLoaderCallBackMethod.callbackAndReset(() -> {
                baseOutput.sendFailedMsg(msg);
                return null;
            }, baseOutput.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
