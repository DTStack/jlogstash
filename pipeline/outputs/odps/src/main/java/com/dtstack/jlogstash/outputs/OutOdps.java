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
package com.dtstack.jlogstash.outputs;

import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.aliyun.odps.Odps;
import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter;
import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.outputs.BaseOutput;
import com.dtstack.jlogstash.render.Formatter;
import com.google.common.collect.Maps;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:35:59
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class OutOdps extends BaseOutput {
	
	    private static Logger logger = LoggerFactory.getLogger(OutOdps.class);

        @Required(required = true)
        private static String accessId;

        @Required(required = true)
        private static String accessKey;

        private static String odpsUrl = "http://service.odps.aliyun.com/api";

        @Required(required = true)
        private static String project;

        @Required(required = true)
        private static String table;

        private static String partition = "jlogstash";//example dt ='dtlog-%{tenant_id}-%{+YYYY.MM.dd}',pt= 'dtlog-%{tenant_id}-%{+YYYY.MM.dd}'

        public static String timezone = null;
        
        private static long bufferSize ;//default 10M
        
        private TableTunnel tunnel;
                
        private static long interval = 5*60*1000;
        
        private long current = System.currentTimeMillis();
           
            
        private Map<String,Object[]> objects = Maps.newHashMap();//[UploadSession,TableSchema,TunnelBufferedWriter,AtomicBoolean]
        
        public OutOdps(Map config) {
                super(config);
                // TODO Auto-generated constructor stub
        }

        @Override
        public void prepare() {
                // TODO Auto-generated method stub
        	try{
        		if(tunnel==null){
        			synchronized(OutOdps.class){
        				if(tunnel==null){
        					tunnel = createTableTunnel();
        				}
        			}
        		}
        	}catch(Throwable e){
        		logger.error("OutOdps init error:",e);
        		System.exit(-1);
        	}
        }
        
        private TunnelBufferedWriter createTunnelBufferedWriter(UploadSession uploadSession) throws TunnelException{
        	TunnelBufferedWriter writer =(TunnelBufferedWriter) uploadSession.openBufferedWriter();
        	if(bufferSize > 0 ){
        		writer.setBufferSize(bufferSize);
        	}
            return writer;
        }
        
        private TableTunnel createTableTunnel(){
        	  Account account = new AliyunAccount(accessId, accessKey);
              Odps odps = new Odps(account);
              odps.setEndpoint(odpsUrl);
              odps.setDefaultProject(project);	
              return new TableTunnel(odps);
        }
        
        public UploadSession createUploadSession(String par) throws TunnelException{
            if(StringUtils.isNotBlank(par)&&!"jlogstash".equals(par)){
                PartitionSpec partitionSpec = new PartitionSpec(par);
                return tunnel.createUploadSession(project,table, partitionSpec);
            }else{
            	return tunnel.createUploadSession(project,table);
            }
        }

        @Override
        protected void emit(Map event) {
                // TODO Auto-generated method stub
        	try {
        		String par = Formatter.format(event, partition, timezone);
        		Object[] objs =  objects.get(par);
        		if(objs == null){
        			objs = createArrayObject(par);
        			objects.put(par, objs);
        		}
        		Record record = createRecord(event,(UploadSession)objs[0],(TableSchema)objs[1]);
        		Object[] cols = record.toArray();
        		if(cols==null||cols.length ==0){
        			return ;
        		}
        		((TunnelBufferedWriter)objs[2]).write(record);
        	    long t1 = System.currentTimeMillis();
        	    if(t1 - current > interval){
        	    	commit();
        	    	objects.clear();
        	    	current = t1;
        	    	logger.warn("uploadSession is committed...");
        	    }
        	} catch (Exception e) {
				// TODO Auto-generated catch block
        		logger.error("OutOdps emit error:",e);
			}
        }
        
        private Object[] createArrayObject(String par) throws TunnelException{
        	UploadSession uploadSession = createUploadSession(par);
        	TableSchema tableSchema = uploadSession.getSchema();
        	TunnelBufferedWriter tunnelBufferedWriter = createTunnelBufferedWriter(uploadSession);
        	return new Object[]{uploadSession,tableSchema,tunnelBufferedWriter};
        }
        
        @Override
        public void release(){
        	commit();
        }
        
        private synchronized void commit(){
        	try{
      		   if(objects.size()>0){
 				   Set<Map.Entry<String,Object[]>> entrys = objects.entrySet();
 				   for(Map.Entry<String,Object[]> entry:entrys){
 					   Object[] objs = entry.getValue();
 					      ((TunnelBufferedWriter)objs[2]).close();
 					      ((UploadSession)objs[0]).commit();
 					   }
 				 }
         	}catch(Throwable e){
         		logger.error("OutOdps commit error:",e);
         	}
        }
        
        private Record createRecord(Map event,UploadSession uploadSession,TableSchema tableSchema){
        	Record record = uploadSession.newRecord();
            for (int i = 0; i < tableSchema.getColumns().size(); i++) {
                Column column = tableSchema.getColumn(i);
                String name = column.getName();
                Object value = event.get(name);
                if(value!=null){
                    switch (column.getType()) {
                    case BIGINT:
                            record.set(name, Long.parseLong(value.toString()));
                            break;
                    case BOOLEAN:
                           record.set(name, Boolean.parseBoolean(value.toString()));
                            break;
                    case DATETIME:
                    	    record.set(name, value);
                            break;
                    case DOUBLE:
                            record.set(name, Double.parseDouble(value.toString()));
                            break;
                    case STRING:
                            record.set(name, value.toString());
                            break;
                    default:
                            logger.warn("{}:Unknown column type: "+ column.getType(),name);
                    }
                  }
               }
            return record;
         }
}
