#说明：
   用java版本重写logstash目的是提升性能,跟ruby logstash 对比 请看 https://github.com/DTStack/jlogstash-performance-testing

   不同的业务，性能会有所不同，dtstack 后台日志解析 java 版本是ruby版本的5倍，在单台4g 4cores 虚拟机上。

   jlogstash 的参数配置和使用看wiki介绍，现在的插件跟ruby版本相比还太少，希望更多的人参与开发。

   各个插件代码在jlogstash-input-plugin，jlogstash-output-plugin，jlogstash-filter-plugin。



#现在已有的公共插件：

##inputs: 详情:https://github.com/DTStack/jlogstash-input-plugin/blob/master/README.md
    Kafka: 
    
    KafkaDistribute:
 
    Stdin: 

    Tcp(mina实现): 

    Netty(netty 实现): 

    Beats:
    
    File:

##filters: 详情: https://github.com/DTStack/jlogstash-filter-plugin/blob/master/README.md
   Add:

   DateISO8601:
 
   IpIp: 

   Grok: 

   Gsub:
 
   Json: 

   KV: 

   Lowercase:
 
   Remove:
 
   Rename:
 
   Replace: 

   Trim:
 
   UA:
 
   Uppercase:
 
   URLDecode:


##outputs: 详情: https://github.com/DTStack/jlogstash-output-plugin/blob/master/README.md
   Elasticsearch5:

   Elasticsearch:
 
   File: 

   Kafka: 

   Netty:

   Performance: 
     记录一段时间内处理的记录数

   Stdout:

#jar放置目录（编译的jar必须要有版本号 ）:
  
    jlogstash 核心代码放在jlogstash/lib/下

    插件的代码分别的放到jlogstash/plugin 下的filter,input,output目录下

#jlogstash 启动参数：

  -f:配置文件 yaml格式路径

  -l:日志文件路径

  -i:input queue size coefficient 默认 200f/1024

  -w:filter work number 默认是根据的机器cpu核数+2

  -o:output work number 默认是根据的机器cpu核数

  -c:output queue size coefficient 默认 500f/1024

  v: error级别
  
  vv: warn级别

  vvv:info级别

  vvvv:debug级别

  vvvvv:trace级别

#插件开发：
  1.现在各自的plugin 的包 都会有各自的classloder去加载，parent classloder是AppClassLoder，所以各自的plugin的代码即使引用了相同的第三的jar版   本不一样也不会导致版本冲突。
  
  2.每一个plugin打的包名的前缀必须跟插件的类名一致，不区分大小写，不然会报类找不到。
  
  3.各个插件的代码不能相互引用，如果有公共代码需要打入到各自的jar包中
  
  4.所需依赖到manven中心库 搜索 jlogstash(http://search.maven.org/ 或https://oss.sonatype.org)
