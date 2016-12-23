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
package com.dtstack.logstash.ua.parser;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.commons.collections.map.LRUMap;

import com.dtstack.logstash.ua.parser.Client;
import com.dtstack.logstash.ua.parser.Device;
import com.dtstack.logstash.ua.parser.OS;
import com.dtstack.logstash.ua.parser.Parser;
import com.dtstack.logstash.ua.parser.UserAgent;

/**
 * When doing webanalytics (with for example PIG) the main pattern is to process
 * weblogs in clickstreams. A basic fact about common clickstreams is that in
 * general the same browser will do multiple requests in sequence. This has the
 * effect that the same useragent will appear in the logfiles and we will see
 * the need to parse the same useragent over and over again.
 *
 * This class introduces a very simple LRU cache to reduce the number of times
 * the parsing is actually done.
 *
 * @author Niels Basjes
 *
 */
public class CachingParser extends Parser {

  // TODO: Make configurable
  private static final int       CACHE_SIZE     = 1000;

  private Map<String, Client>    cacheClient    = null;
  private Map<String, UserAgent> cacheUserAgent = null;
  private Map<String, Device>    cacheDevice    = null;
  private Map<String, OS>        cacheOS        = null;

  // ------------------------------------------

  public CachingParser() throws IOException {
    super();
  }

  public CachingParser(InputStream regexYaml) {
    super(regexYaml);
  }

  // ------------------------------------------

  @SuppressWarnings("unchecked")
  @Override
  public Client parse(String agentString) {
    if (agentString == null) {
      return null;
    }
    if (cacheClient == null) {
      cacheClient = new LRUMap(CACHE_SIZE);
    }
    Client client = cacheClient.get(agentString);
    if (client != null) {
      return client;
    }
    client = super.parse(agentString);
    cacheClient.put(agentString, client);
    return client;
  }

  // ------------------------------------------

  @SuppressWarnings("unchecked")
  @Override
  public UserAgent parseUserAgent(String agentString) {
    if (agentString == null) {
      return null;
    }
    if (cacheUserAgent == null) {
      cacheUserAgent = new LRUMap(CACHE_SIZE);
    }
    UserAgent userAgent = cacheUserAgent.get(agentString);
    if (userAgent != null) {
      return userAgent;
    }
    userAgent = super.parseUserAgent(agentString);
    cacheUserAgent.put(agentString, userAgent);
    return userAgent;
  }

  // ------------------------------------------

  @SuppressWarnings("unchecked")
  @Override
  public Device parseDevice(String agentString) {
    if (agentString == null) {
      return null;
    }
    if (cacheDevice == null) {
      cacheDevice = new LRUMap(CACHE_SIZE);
    }
    Device device = cacheDevice.get(agentString);
    if (device != null) {
      return device;
    }
    device = super.parseDevice(agentString);
    cacheDevice.put(agentString, device);
    return device;
  }

  // ------------------------------------------

  @SuppressWarnings("unchecked")
  @Override
  public OS parseOS(String agentString) {
    if (agentString == null) {
      return null;
    }

    if (cacheOS == null) {
      cacheOS = new LRUMap(CACHE_SIZE);
    }
    OS os = cacheOS.get(agentString);
    if (os != null) {
      return os;
    }
    os = super.parseOS(agentString);
    cacheOS.put(agentString, os);
    return os;
  }

  // ------------------------------------------

}
