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
package com.dtstack.jlogstash.factory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.jlogstash.annotation.plugin.AnnotationInterface;
import com.dtstack.jlogstash.property.SystemProperty;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:26:33
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public abstract class InstanceFactory {
	
	private static Logger logger = LoggerFactory.getLogger(InstanceFactory.class);
	
	protected static Map<String,ClassLoader> classCloaders = null;
	
	@SuppressWarnings("rawtypes")
	protected static void configInstance(Class<?> clasz,Map config) throws Exception{
		Field[] fields =clasz.getDeclaredFields();
		if(config!=null&&fields!=null&&fields.length>0){
			for(Field field:fields){
				if((field.getModifiers() & java.lang.reflect.Modifier.STATIC) == java.lang.reflect.Modifier.STATIC){			
					String name =field.getName();
					Object obj =config.get(name);
					if(obj!=null){
						Annotation[] ans = field.getAnnotations();
						if(ans!=null){
							for(Annotation an:ans){
								if(an!=null){
									String annotationPackage = SystemProperty.getSystemProperty("annotationPackage");
									Package pack = an.annotationType().getPackage();
									if(pack!=null){
										if(annotationPackage.equals(pack.getName())){
											logger.warn("field: {} annotation:{} check",name,an.annotationType().getSimpleName());
											checkAnnotation(field,an,obj);
										}
									}
								}
							}
						}				
						field.setAccessible(true);
						field.set(null, obj);
					}
				}
			}
		}
	}
	
	
	@SuppressWarnings("rawtypes")
	protected static void configInstance(Object instance,Map config) throws Exception{
		Field[] fields =instance.getClass().getDeclaredFields();
		if(config!=null&&fields!=null&&fields.length>0){
			for(Field field:fields){
				if((field.getModifiers() & java.lang.reflect.Modifier.STATIC) != java.lang.reflect.Modifier.STATIC){			
					String name =field.getName();
					Object obj =config.get(name);
					if(obj!=null){
						Annotation[] ans = field.getAnnotations();
						if(ans!=null){
							for(Annotation an:ans){
								if(an!=null){
									String annotationPackage = SystemProperty.getSystemProperty("annotationPackage");
									Package pack = an.annotationType().getPackage();
									if(pack!=null){
										if(annotationPackage.equals(pack.getName())){
											logger.warn("field: {} annotation:{} check",name,an.annotationType().getSimpleName());
											checkAnnotation(field,an,obj);
										}
									}
								}
							}
						}				
						field.setAccessible(true);
						field.set(instance, obj);
					}
				}
			}
		}
	}
	
	protected static Class<?> getPluginClass(String type,String pluginType) throws ClassNotFoundException{
		String className = com.dtstack.jlogstash.utils.Package.getRealClassName(type, pluginType);
		String[] names = type.split("\\.");
		String key = String.format("%s:%s",pluginType, names[names.length-1].toLowerCase());
		if(classCloaders!=null&&classCloaders.size()>0){
			ClassLoader cc = classCloaders.get(key);
			if(cc!=null)return cc.loadClass(className);
		}
		logger.warn("plugin classLoadder is AppClassLoader");
		return Thread.currentThread().getContextClassLoader().loadClass(className);
	}
	
	private static void checkAnnotation(Field field,Annotation an,Object obj) throws Exception {
		String className = SystemProperty.getSystemProperty("annotationPlugin")+"."+an.annotationType().getSimpleName()+"Plugin";
		Class<?> cla =null;
		try{
			 cla = Class.forName(className);
		}catch(ClassNotFoundException ex){
			logger.error("className:{} not found: {}",className,ex.getCause());
			return;
		}
		AnnotationInterface annotationInterface = (AnnotationInterface) cla.newInstance();	
		annotationInterface.required(field, obj);
	}

	public static void setClassCloaders(Map<String, ClassLoader> classCloaders) {
//		important
		if(classCloaders!=null&&classCloaders.size()>0){
			Thread.currentThread().setContextClassLoader(null);
		}
		if (InstanceFactory.classCloaders ==null)InstanceFactory.classCloaders = classCloaders;
	}
}
