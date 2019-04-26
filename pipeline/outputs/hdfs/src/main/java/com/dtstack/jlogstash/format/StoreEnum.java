package com.dtstack.jlogstash.format;

/**
 * 
 * @author sishu.yss
 *
 */
public enum StoreEnum {
	
	TEXT,ORC;

	public static String listStore(){
		StringBuilder sb = new StringBuilder();
		for (StoreEnum store : StoreEnum.values()) {
			sb.append(store.name());
			sb.append(",");
		}
		sb.setCharAt(sb.length() - 1,' ');
		return sb.toString();
	}
}
