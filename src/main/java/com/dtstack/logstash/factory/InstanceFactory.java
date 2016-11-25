package com.dtstack.logstash.factory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.logstash.annotation.plugin.AnnotationInterface;
import com.dtstack.logstash.property.SystemProperty;

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
}
