# Beats:
   codec:默认plain

   prot: 端口必填没有默认值

   host: ip地址，默认 0.0.0.0
   
   addFields: 需要添加的属性，map 结构

# Kafka09:
   encoding:编码 默认 utf8

   codec:默认plain
 
   topic:必填，map结构，需要说明分区数（{dt_all_test_log: 6}）

   consumerSettings:必填 consumer 连接kafka的属性配置，map结构 {group.id: jlogstashvvvvv,zookeeper.connect: 127.0.0.1:2181,auto.commit.interval.ms:"1000",auto.offset.reset: smallest}

   addFields: 需要添加的属性，map 结构
   
# Kafka10:

   codec:默认plain
 
   topic:必填，string
   
   groupId:必填，string

   consumerSettings:必填 consumer 连接kafka的属性配置，map结构 {zookeeper.connect: 127.0.0.1:2181,auto.commit.interval.ms:"1000",auto.offset.reset: smallest}
   
   bootstrapServers: 必填 127.0.0.1:9020,127.0.0.2:9020

   addFields: 需要添加的属性，map 结构    
   
# KafkaDistribute:
   encoding:编码 默认 utf8

   codec:默认plain
 
   topic:必填，map结构，需要说明分区数（{dt_all_test_log: 6}）

   consumerSettings:必填 consumer 连接kafka的属性配置，map结构 {group.id: jlogstashvvvvv,zookeeper.connect: 127.0.0.1:2181,auto.commit.interval.ms:"1000",auto.offset.reset: smallest}
   
   addFields: 需要添加的属性，map 结构 
   
  distributed: map结构，属性值不为空 说明要开启分布式（主要的应用场景是单个文件日志无序，有单行，有多行，需要后台做聚合和解析，需要把同一个日志发送     到同一台服务器）每种日志需要定制化开发聚合规则，现在的版本里有cms1.8 gc log 的日志聚合规则
    样本: {"zkAddress":"127.0.0.1:2181/distributed","localAddress":"127.0.0.1:8555","hashKey":"%{tenant_id}:%                {hostname}_%{appname}_%{path}"}
   
     
# Netty:
  codec:默认plain

  prot: 端口必填没有默认值

  host: ip地址，默认 0.0.0.0

  encoding:编码 默认 utf8

  codec:默认plain

  receiveBufferSize:接收缓存区大小 默认值20M

  delimiter:数据的分隔符 默认是根据系统的换行分隔符

  addFields: 需要添加的属性，map 结构
  
  whiteListPath: ip白名单路径（多个用逗号隔开）
  
  isExtract: true|false是否开启解压功能（gzip）

# Tcp:
  
  codec:默认plain

  prot: 端口必填没有默认值

  host: ip地址，默认 0.0.0.0

  encoding:编码 默认 utf8

  bufSize: 接收缓存区大小 默认值20M

  maxLineLength:一次接收最大的数据包大小 默认1M

  addFields: 需要添加的属性，map 结构

# Stdin:
  标准输入
  addFields: 需要添加的属性，map 结构


# File:
 addFields: 需要添加的属性，map 结构

 path:文件输入路径(可以是文件,文件夹),参数类型为list["home/admin/ysq.log"]

 pathcodecMap: 文件路径,参数是Map类型(key:文件路径,value:该文件类型对应的codec)

 --注意path参数和pathcodecMap参数不能同时为空

 exclude:排除文件路径(可以是文件,文件夹),参数类型为list

 encoding:读取文件的编码格式,默认是UTF-8

 maxOpenFiles:最大配置读取文件数量,默认为0(表示无上限)

 startPosition: 文件开始读取位置,["beginning", "end"],默认为end

 sinceDbPath:文件读取位置信息存储位置,默认"./sincedb.yaml"

 sinceDbWriteInterval:文件读取位置信息刷新到存储点的时间间隔,默认是15s

 delimiter:行分割符号,默认是'\n'

 readFileThreadNum:文件读取的线程数,默认是:cpu处理器数+1
 
 # Redis:
 host:redis服务主机地址
 
 key:键值，当data_type为channel或者channel_pattern时表示订阅的频道
 
 data_size:数据数量，只有当data_type为list和sorted_set时有效
 
 data_type:数据格式，取值范围为：string,list,set,sorted_set,hash,channel,channel_pattern,其中channel和channel_pattern表示订阅模式下所监听的频道
 
# Elasticsearch:
  hosts: elasticsearch 集群地址，也可以slb地址 类型是数组（["node01","node02"]）必填
  
  cluster: elasticsearch 集群名称(默认值 elasticsearch)
  
  sniff:是否自动发现（默认值 true）
  
  index: 索引（默认值 logstash-*）
  
  type: 索引类型
  
  query: dsl 查询语法（默认 {\"query\": {\"match_all\":{}},\"sort\" : [\"_doc\"]}）
  
  scroll: 翻页（默认 5）
  
  size: 一次获取的数据 （默认 1000）
  
  user: 用户名
  
  password: 密码
  
# Jdbc:
  jdbcConnectionString: jdbc url地址 必填项
  
  jdbcDriverClass: 驱动类 必填项
  
  jdbcDriverLibrary: 驱动包的路径 必填项
  
  jdbcFetchSize: 一次获取的数据 必填项
  
  jdbcUser: 用户名 必填项
  
  jdbcPassword: 密码 必填项
  
  statement: 查询语句 必填项
  
  parameters: 参数

# MongoDB:
  uri: MongoDB连接URI 必填项
  
  dbName: database名称 必填项
  
  collection: collection名称 必填项
  
  query: Filter语句 
  
  sinceTime: 增量抽取的起始时间

# Binlog
  host: MySQL主机名 必填项

  port: MySQL端口号 默认3306

  username: MySQL用户名 必填项

  password: MySQL密码 必填项

  start: 日志起始位置， 格式为 {"journalName":"mysql-bin.000002","position":39493,timestamp":1537948008000}，其中journalName为binlog日志文件名，position为日志偏移量，timestamp为日志时间戳

  filter: 过滤器列表，由若干个过滤器组成， 格式为 {schema1\.table1,schema2\.table2}，多个过滤器之间用逗号分隔；
  默认为空，表示不过滤schema和table。

  cat: 数据操作类别列表，由若干个数据操作类别组成，格式为 {insert,update,select,delete}，多个数据操作类别用逗号分隔；
  默认为空，表示处理binlog所有操作类别的日志。
