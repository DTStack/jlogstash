package com.dtstack.jlogstash.format.util;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.SimpleTimeZone;


public class DateUtil {

    static final String timeZone = "GMT+8";
    static final String datetimeFormat = "yyyy-MM-dd HH:mm:ss";
    static final String dateFormat = "yyyy-MM-dd";
    static final String timeFormat = "HH:mm:ss";
    static final SimpleDateFormat datetimeFormatter = new SimpleDateFormat(datetimeFormat);
    static final SimpleDateFormat dateFormatter = new SimpleDateFormat(dateFormat);
    static final SimpleDateFormat timeFormatter = new SimpleDateFormat(timeFormat);

    public static java.sql.Date columnToDate(Object column) {
        if(column instanceof String) {
            return new java.sql.Date(stringToDate((String)column).getTime());
        } else if (column instanceof Integer) {
            Integer rawData = (Integer) column;
            return new java.sql.Date(rawData.longValue());
        } else if (column instanceof Long) {
            Long rawData = (Long) column;
            return new java.sql.Date(rawData.longValue());
        } else if (column instanceof java.sql.Date) {
            return (java.sql.Date) column;
        } else if(column instanceof Timestamp) {
            Timestamp ts = (Timestamp) column;
            return new java.sql.Date(ts.getTime());
        }
        throw new IllegalArgumentException("Can't convert " + column.getClass().getName() + " to Date");
    }

    public static Date stringToDate(String strDate)  {
        if(strDate == null)
        {
            return null;
        }
        try {
            return datetimeFormatter.parse(strDate);
        } catch (ParseException ignored) {
        }

        try {
            return dateFormatter.parse(strDate);
        } catch (ParseException ignored) {
        }

        try {
            return timeFormatter.parse(strDate);
        } catch (ParseException ignored) {
        }

        throw new RuntimeException("can't parse date");
    }


    public static long getTodayStart(long day) {
        long firstDay = 0L;
        Calendar cal = Calendar.getInstance();
        if (("" + day).length() > 10) {
            cal.setTime(new Date(day));
        } else {
            cal.setTime(new Date(day * 1000));
        }
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        firstDay = cal.getTimeInMillis() / 1000;
        return firstDay;
    }


    public static long getTodayStart(long day,String scope) {
    	if(scope.equals("MS")){
    		return getTodayStart(day)*1000;
    	}else if(scope.equals("S")){
    		return getTodayStart(day);
    	}else{
    		return getTodayStart(day);
    	}
    }


    public static long getNextDayStart(long day) {
        long daySpanMill = 86400000L;
        long nextDay = 0L;
        Calendar cal = Calendar.getInstance();
        if (("" + day).length() > 10) {
            cal.setTime(new Date(day));
        } else {
            cal.setTime(new Date(day * 1000));
        }
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        nextDay = (cal.getTimeInMillis() + daySpanMill) / 1000;
        return nextDay;
    }
    

    public static long getNextDayStart(long day,String scope) {
    	if(scope.equals("MS")){
    		return getNextDayStart(day)*1000;
    	}else if(scope.equals("S")){
    		return getNextDayStart(day);
    	}else{
    		return getNextDayStart(day);
    	}
    }
    


    public static long getMonthFirst(long day) {
        long firstDay = 0L;
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(day * 1000));
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        firstDay = cal.getTimeInMillis() / 1000;
        return firstDay;
    }


    public static int getMonth(long day) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(day * 1000));
        return cal.get(Calendar.MONTH) + 1;
    }


    public static int getYear(long day) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(day * 1000));
        return cal.get(Calendar.YEAR);
    }


    public static long getWeekFirst(long day) {
        long firstDay = 0L;
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(day * 1000));
        cal.setFirstDayOfWeek(Calendar.MONDAY);
        cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        firstDay = cal.getTimeInMillis() / 1000;
        return firstDay;
    }


    public static int getWeekOfYear(long day) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(day * 1000));
        return cal.get(Calendar.WEEK_OF_YEAR);
    }


    public static String getYesterdayByString(String day, String inFormat, String outFormat){
        try {
			SimpleDateFormat sdf = new SimpleDateFormat(inFormat);
			Date date = sdf.parse(day);
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			int calendarDay = calendar.get(Calendar.DATE);
			calendar.set(Calendar.DATE, calendarDay - 1);
			String dayBefore = new SimpleDateFormat(outFormat).format(calendar.getTime());
			return dayBefore;
		} catch (ParseException e) {
			return null;
		}
    }


    public static String getTomorrowByString(String day, String inFormat, String outFormat) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(inFormat);
        Date date = sdf.parse(day);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int calendarDay = calendar.get(Calendar.DATE);
        calendar.set(Calendar.DATE, calendarDay + 1);
        String dayBefore = new SimpleDateFormat(outFormat).format(calendar.getTime());
        return dayBefore;
    }
    

    public static Date getTomorrowByDate(Date date) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int calendarDay = calendar.get(Calendar.DATE);
        calendar.set(Calendar.DATE, calendarDay + 1);
        return calendar.getTime();
    }


    public static String get30DaysBeforeByString(String day, String inFormat, String outFormat) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(inFormat);
        Date date = sdf.parse(day);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int calendarDay = calendar.get(Calendar.DATE);
        calendar.set(Calendar.DATE, calendarDay - 30);
        String dayBefore = new SimpleDateFormat(outFormat).format(calendar.getTime());
        return dayBefore;
    }
    

    public static String get30DaysLaterByString(String day, String inFormat, String outFormat) throws ParseException {
    	SimpleDateFormat sdf = new SimpleDateFormat(inFormat);
    	Date date = sdf.parse(day);
    	Calendar calendar = Calendar.getInstance();
    	calendar.setTime(date);
    	int calendarDay = calendar.get(Calendar.DATE);
    	calendar.set(Calendar.DATE, calendarDay + 30);
    	String dayBefore = new SimpleDateFormat(outFormat).format(calendar.getTime());
    	return dayBefore;
    }



    public static String getDateStrTOFormat(String day, String inFormat, String outFormat) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(inFormat);
        Date date = sdf.parse(day);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        String dayBefore = new SimpleDateFormat(outFormat).format(calendar.getTime());
        return dayBefore;
    }
    
    public static long getDateMillTOFormat(String day, String inFormat) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(inFormat);
        Date date = sdf.parse(day);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.getTimeInMillis()/1000;
    }


    public static long getFirstDay4Month(int year, int month) {
        long firstDay = 0L;
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, year);
        // 上一个月的最后一天的下一天：这个月的第一天
        cal.set(Calendar.MONTH, month - 1);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        firstDay = cal.getTimeInMillis() / 1000;
        return firstDay;
    }


    public static long getLastDay4Month(int year, int month) {
        long lastDay = 0L;
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.MONTH, month);
        // 1表示下个月第一天的零点，可以看做这个月的最有一天结束,但是数据表就最后一天零点就行
        cal.set(Calendar.DAY_OF_MONTH, 0);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        lastDay = cal.getTimeInMillis() / 1000;
        return lastDay;
    }


    public static long getBeforeMonthDay(long day, boolean chooseFirstDay) {
        long chooseDay = 0L;
        int currentMonth = getMonth(day);
        int currentYear = getYear(day);
        if (currentMonth > 1) {
            currentMonth--;
        } else {
            currentYear--;
            currentMonth = 12;
        }
        if (chooseFirstDay) {
            chooseDay = getFirstDay4Month(currentYear, currentMonth);
            return chooseDay;
        } else {
            chooseDay = getLastDay4Month(currentYear, currentMonth);
            return chooseDay;
        }

    }


    public static long getMillByOneDay() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis() / 1000;
    }


    public static long getMillByYesDay() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.DATE, cal.get(Calendar.DATE) - 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis() / 1000;
    }


    public static long getMillByLastWeekDay() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.DATE, cal.get(Calendar.DATE) - 7);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis() / 1000;
    }
    

    public static long getMillByDay(int severalDays,String condition) {
    	int dateT=0;
        Calendar cal = Calendar.getInstance();
    	if(condition==null){
    		return getMillToDay(cal,dateT);
    	}
        if(condition.equals("-")){
        	dateT = (cal.get(Calendar.DATE) - severalDays);
        	return getMillToDay(cal,dateT);
        }
        if(condition.equals("+")){
        	dateT = (cal.get(Calendar.DATE) + severalDays);
        	return getMillToDay(cal,dateT);
        }
		return getMillToDay(cal,dateT);
  }
    

    public static long getStampByDay(int severalDays,String condition) {
    	int dateT=0;
    	Calendar cal = Calendar.getInstance();
    	if(condition==null){
    		return getStampToDay(cal,dateT);
    	}
    	if(condition.equals("-")){
    		dateT = (cal.get(Calendar.DATE) - severalDays);
    		return getStampToDay(cal,dateT);
    	}
    	if(condition.equals("+")){
    		dateT = (cal.get(Calendar.DATE) + severalDays);
    		return getStampToDay(cal,dateT);
    	}
    	return getStampToDay(cal,dateT);
    }

    
    public static long getMillByDay(){
		return getMillByDay(0,null);
    }
    

    public static long getMillToDay(Calendar cal,int dateT){
		   if(dateT!=0){
			   cal.set(Calendar.DATE, dateT);
		   }
	       cal.set(Calendar.HOUR_OF_DAY, 0);
	       cal.set(Calendar.MINUTE, 0);
	       cal.set(Calendar.SECOND, 0);
	       cal.set(Calendar.MILLISECOND, 0);
	       return cal.getTimeInMillis()/1000;
	}
    

    public static long getStampToDay(Calendar cal,int dateT){
    	if(dateT!=0){
    		cal.set(Calendar.DATE, dateT);
    	}
    	return cal.getTimeInMillis();
    }

    public static String getToday() {
        Calendar cal = Calendar.getInstance();
        return cal.get(1) + "年" + cal.get(2) + "月" + cal.get(3) + "日";
    }


    public static String getDate(long day, String format) {
        Calendar cal = Calendar.getInstance();
        if (("" + day).length() > 10) {
            cal.setTime(new Date(day));
        } else {
            cal.setTime(new Date(day * 1000));
        }
        SimpleDateFormat sf = new SimpleDateFormat(format);
        return sf.format(cal.getTime());
    }
    

    public static String getDate(Date date, String format) {
        SimpleDateFormat sf = new SimpleDateFormat(format);
        return sf.format(date);
    }
    
    

    public static long stringToLong(String day, String format) throws ParseException {
    	SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        long Date = dateFormat.parse(day).getTime();
    	return Date;
    }
    

    public static Date stringToDate(String day, String format)  {
    	try {
			SimpleDateFormat dateFormat = new SimpleDateFormat(format);
			 Date Date = dateFormat.parse(day);
			return Date;
		} catch (ParseException e) {
			return new Date();
		}
    }
    
    

    public static String longToString(long day, String format) throws ParseException {
    	if (("" + day).length() <= 10)
        {
            day=day*1000;
        }
    	SimpleDateFormat dateFormat = new SimpleDateFormat(format);
	    String Date = dateFormat.format(day);
    	return Date;
    }


    public static int getMinusDate(int day, int minusDay) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(day * 1000));
        cal.set(Calendar.DATE, cal.get(Calendar.DATE) - minusDay);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return (int) cal.getTimeInMillis() / 1000;
    }


    public static long getMillByNow() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        return cal.getTimeInMillis();
    }

	public static int getWeeksBetweenTwoDates(long startDay, long endDay) {
		int week = getWeekOfYear(endDay) - getWeekOfYear(startDay) + 1;
		if(week<1){
			week = getWeekOfYear(endDay) + getMaxWeekOfYear(startDay) - getWeekOfYear(startDay) + 1;
		}
		return week;
	}

	public static int getMaxWeekOfYear(long startDay) {
		Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(startDay * 1000));
        return cal.getMaximum(Calendar.WEEK_OF_YEAR);
	}
	
	public static int getMonthsBetweenTwoDates(long startDay, long endDay) {
		int month = DateUtil.getMonth(endDay) - DateUtil.getMonth(startDay) + 1;
		if(month<1){
			month = getMonth(endDay) + 12 - getMonth(startDay) +1;
		}
		return month;
	}
	
	public static Date parseDate(String dateStr, String pattern){
		SimpleDateFormat sdf = new SimpleDateFormat();
		sdf.applyPattern(pattern);
		try {
			return sdf.parse(dateStr);
		} catch (ParseException e) {
			return null;
		}
	}
	
    public static long getMinuteStart(long time) {
        long firstDay = 0L;
        Calendar cal = Calendar.getInstance();
        if (("" + time).length() > 10) {
            cal.setTime(new Date(time));
        } else {
            cal.setTime(new Date(time * 1000));
        }
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        firstDay = cal.getTimeInMillis() / 1000;
        return firstDay;
    }
    
    public static long getHourStart(long time) {
        long firstDay = 0L;
        Calendar cal = Calendar.getInstance();
        if (("" + time).length() > 10) {
            cal.setTime(new Date(time));
        } else {
            cal.setTime(new Date(time * 1000));
        }
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.MINUTE, 0);
        firstDay = cal.getTimeInMillis() / 1000;
        return firstDay;
    }


    public static Date getDateByLong(long time){
        Date date = new Date();
        date.setTime(time);
        return date;
    }
    

    public static Date parseDate(String dateStr, String pattern, Locale locale){
    	SimpleDateFormat df = new SimpleDateFormat(
				pattern, locale);

		df.setTimeZone(new SimpleTimeZone(0, "GMT"));
		try {
			return df.parse(dateStr);
		} catch (ParseException e) {
			return null;
		}
	}
    
    public static String getDate(Date date, String format, Locale locale) {
    	SimpleDateFormat df = new SimpleDateFormat(
    			format, locale);
    	df.setTimeZone(new SimpleTimeZone(0, "GMT"));
        return df.format(date);
    }
}
