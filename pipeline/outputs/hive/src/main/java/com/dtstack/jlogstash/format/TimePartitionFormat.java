package com.dtstack.jlogstash.format;


import com.dtstack.jlogstash.format.util.DateUtil;

import java.util.Date;

/**
 * @author toutian
 */

public class TimePartitionFormat {

    private static PartitionEnum partitionEnum;

    private static TimePartitionFormat timePartitionFormat = new TimePartitionFormat();

    public static TimePartitionFormat getInstance(PartitionEnum pe) {
        partitionEnum = pe;
        return timePartitionFormat;
    }

    public static TimePartitionFormat getInstance(String peStr) {
        if (PartitionEnum.DAY.name().equalsIgnoreCase(peStr)) {
            partitionEnum = PartitionEnum.DAY;
        } else if (PartitionEnum.HOUR.name().equalsIgnoreCase(peStr)) {
            partitionEnum = PartitionEnum.HOUR;
        } else {
            throw new UnsupportedOperationException("partitionEnum=" + peStr + " is undefined!");
        }
        return timePartitionFormat;
    }

    private TimePartitionFormat() {
    }

    public String currentTime() {
        if (PartitionEnum.DAY == partitionEnum) {
            return DateUtil.getDayFormatter().format(new Date());
        } else if (PartitionEnum.HOUR == partitionEnum) {
            return DateUtil.getHourFormatter().format(new Date());
        }

        throw new UnsupportedOperationException("partitionEnum=" + partitionEnum + " is undefined!");
    }

    enum PartitionEnum {
        DAY, HOUR
    }

}
