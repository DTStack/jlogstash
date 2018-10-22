
# Elasticsearch:

      index:索引(dtlog-%{tenant_id}-%{+YYYY.MM.dd}) 必填
    
      indexTimezone: 如果索引有时间，配置时区 默认 UTC

      documentId:文档id
    
      documentType: 文档类型 默认 logs;
    
      cluster:集群名称
    
      hosts:ip地址，带的端口是tcp端口，数组结构（["172.16.1.185:9300","172.16.1.188:9300"]）必填
    
      sniff:默认true
    
      bulkActions: 默认 20000 
    
      bulkSize:默认 15

      consistency:false 数据一致性的开关，默认关闭；打开之后，在elasticsearch 集群不可用的情况下，数据会不断重试，不会再消费input数据，直到elasticsearch集群可用

# Elasticsearch5:

      index:索引(dtlog-%{tenant_id}-%{+YYYY.MM.dd}) 必填
    
      indexTimezone: 如果索引有时间，配置时区 默认 UTC

      documentId:文档id
    
      documentType: 文档类型 默认 logs;
    
      cluster:集群名称
    
      hosts:ip地址，带的端口是tcp端口，数组结构（["172.16.1.185:9300","172.16.1.188:9300"]）必填
    
      sniff:默认true
    
      bulkActions: 默认 20000 
    
      bulkSize:默认 15

      consistency:false 数据一致性的开关，默认关闭；打开之后，在elasticsearch 集群不可用的情况下，数据会不断重试，不会再消费input数据，直到elasticsearch集群可用


# Kafka:

    encoding:默认utf-8
    
    topic:必填(dt-%{tenant_id})

    brokerList:kafka集群地址，多个逗号隔开（12.24.36.128:9092,11.37.67.213:9092）

    keySerializer: 默认值 kafka.serializer.StringEncoder 可以自定义
	
    valueSerializer:默认值 kafka.serializer.StringEncoder	 可以自定义
	
    partitionerClass:默认值 kafka.producer.DefaultPartitioner  可以自定义
	 
    producerType:默认值 "sync" //sync,async 可选
	 
    compressionCodec: "none" //gzip,snappy,lz4,none
	 
    clientId 默认没有
	
    batchNum 默认kafka自带的值
	
    requestRequiredAcks 默认值为1
    
# OutOdps:

    accessId: aliyun accessId 需要到阿里云官网申请 （必填）
    
    accessKey: aliyun accessKey 需要到阿里云官网申请（必填）
    
    odpsUrl: http://service.odps.aliyun.com/api（默认值）
    
    project: odps 项目(必填)
    
    table: odps 项目里表(必填)
    
    partition: 表分区，支持静态分区和动态分区  dt ='dtlog-%{tenant_id}-%{+YYYY.MM.dd}',pt= 'dtlog-%{tenant_id}-%{+YYYY.MM.dd}'
    
    bufferSize: default 10M 
    
    interval: default 300000 mills
    
# Performance:

   interval: 数据刷入文件的间隔时间，默认30秒

   timeZone: 时区 默认UTC

   path: 文件路径（home/admin/jlogserver/logs/srsyslog-performance-%{+YYYY.MM.dd}.txt）必填

# File:

   timeZone:时区 默认UTC

   path:文件路径（home/admin/jlogserver/logs/srsyslog-performance-%{+YYYY.MM.dd}.txt）必填

   codec:默认是json_lines(可选值：line(可以自定义输出的属性和属性之间的分隔符)，json_lines（json格式的字符串格式输出）)
   
   format:自定义输出的格式（tenant_id|ip）
   
   split:自定义输出格式属性之间的分隔符

# Stdout:

  codec:line(默认值)
  
  line,json_lines, java_lines三种值可以选择


# Netty 

  host:连接远程的ip 必填
	
  port:连接远程的端口 必填

  openCompression: 是否开启数据压缩--开启之后会使用本地的缓存,达到设定的时间或者长度之后才会发送,默认false

  compressionLevel: 压缩等级，使用gzip压缩，默认是6

  sendGapTime：使用本地缓存的时候最大缓存时间，超过设定时间将会发送, 默认值：2 * 1000(ms)

  maxBufferSize：使用本地缓存的时候的最大缓存大小，超过设定大小的时候会发送,默认值：5 * 1024 字符

  openCollectIp: 是否获取本地的ip地址添加到消息里

  format：输出数据格式，eg:${HOSTNAME} ${appname} [${user_token} type=${logtype} tag="${logtag}"],会将对应的变量名称替换成消息里面的存在值

  delimiter: 发送的字符串的分隔符，默认是系统行分隔符
  

# Hdfs 

  hadoopConf:hadoop 配置文件目录（默认读取环境变量HADOOP_CONF_DIR）
	
  path:写入hdfs路径目录 必填

  store: 存储类型（现在支持text，orc）

  compression：数据写入的压缩类型（NONE,GZIP,BZIP2,SNAPPY）

  charsetName: 字符集（默认 utf-8）

  delimiter：分隔符（text 类型适用）

  timezone: 时区
  
  hadoopUserName ： 访问hadoop 的用户名
  
  schema：写入hadoop的数据格式（["name:varchar"]）
  
