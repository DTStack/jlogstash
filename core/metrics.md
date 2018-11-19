
> 性能指标介绍 

* input 采集下列能数据
    * 累计类型
        1. numRecordsInLocal（输入总数）
        2. numBytesIn（输入总字节数）
    * 速率类型
        1. numRecordsInPerSecond（每秒输入的数量）
        2. numBytesInLocalPerSecond（每秒输入的字节数）
        
* output 采集下列能数据
    * 累计类型
        1. 输出总数（numRecordsOut）
        2. 输出总字节数（numBytesOut）
    * 速率类型
        1. 每秒输出的数量（numRecordsOutPerSecond）
        2. 每秒输出的字节数（numBytesOutPerSecond）

* JVM 采集下列runtime数据
    * ClassLoader
        * ClassesLoaded
        * ClassesUnloaded
    * GarbageCollector
        * Count
        * Time
    * Memory
        * Heap
            * Used 
            * Committed
            * Max
        * NonHeap
            * Used 
            * Committed
            * Max
        * Direct
            * Count
            * MemoryUsed
            * TotalCapacity
        * Mapped
            * Count
            * MemoryUsed
            * TotalCapacity
    * Threads
        * Count
    * CPU
        * Load
        * Time

# Promethues:

    ``` 配置示例
    
        {
            "input": {...},
            "output": {...},
            "metrics": [
                {
                    "Prometheus": {
                        "host": "172.16.8.106",
                        "jobName": "devAcquire",
                        "interval": "5+SECONDS",
                        "class": "com.dtstack.jlogstash.metrics.promethues.PrometheusPushGatewayReporter",
                        "port": "9091"
                    }
                }
            ]
        }
    
    ``
    
    host : ip 地址
    port : port 端口
    jobName : 任务名称
    interval : 采集间隔
    class : 采集方式
        1. 拉模式 com.dtstack.jlogstash.metrics.promethues.PrometheusReporter 
        2. 推模式 com.dtstack.jlogstash.metrics.promethues.PrometheusPushGatewayReporter