#说明：
   用java版本重写logstash目的是提升性能,跟ruby logstash 对比 请看 https://github.com/DTStack/jlogstash-performance-testing

   不同的业务，性能会有所不同，dtstack 后台日志解析 java 版本是ruby版本的5倍，在单台4g 4cores 虚拟机上。

   jlogstash 的参数配置和使用看wiki介绍，现在的插件跟ruby版本相比还太少，希望更多的人参与开发。

   各个插件代码在jlogstash-input-plugin，jlogstash-output-plugin，jlogstash-filter-plugin。



#现在已有的公共插件：

##inputs: 
    Kafka: 
 
    Stdin: 

    Tcp(mina实现): 

    Netty(netty 实现): 

    Beats:
    
    File:

##filters:
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


##outputs:
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

-e:开发环境 dev 生产环境 pro（默认生产环境）

-v: info级别

-vv:debug级别

-vvv:trace级别

-vvv:trace级别

#注释：
  现在各自的plugin 的包 都会有各自的classloder去加载，parent classloder是appclassloder，所以各自的plugin的代码即使引用了相同的第三的jar版本不一样也不会导致版本冲突。
  每一个plugin打的包名的前缀必须跟插件的类名一直，不区分大小写，不然会报类找不到。
