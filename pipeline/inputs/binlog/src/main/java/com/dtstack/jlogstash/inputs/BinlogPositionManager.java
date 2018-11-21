package com.dtstack.jlogstash.inputs;


import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.index.AbstractLogPositionManager;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinlogPositionManager extends AbstractLogPositionManager {

    private static final Logger logger = LoggerFactory.getLogger(BinlogPositionManager.class);

    private final Binlog binlog;

    public BinlogPositionManager(Binlog binlog) {
        this.binlog = binlog;
    }

    @Override
    public LogPosition getLatestIndexBy(String destination) {
        return null;
    }

    @Override
    public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
        if(logger.isDebugEnabled()){
            logger.debug("persistLogPosition: " + logPosition.toString());
        }
        binlog.updateLastPos(logPosition.getPostion());
    }

}
