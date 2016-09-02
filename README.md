注释：
   由于ruby版的logstash在性能上遇到瓶颈（现在云日志后端解析日志的能力是800w/h），所以用java重写logstash，整体的设计框架跟ruby版的是一样的。重写的性能大概是4000w/h，比之前的提升了5倍左右。压测的机器配置是虚拟机4g 4core。
  
现在已有的公共插件：
  
  inputs:
     Kafka
     Stdin
     Tcp(mina实现)
     Netty(netty 实现)
     Beats
   
  filters:
    Add
    DateISO8601
    Grok
    Gsub
    Json
    KV
    Lowercase
    Remove
    Rename
    Replace
    Trim
    UA
    Uppercase
    URLDecode
    
  outputs:
    Elasticsearch
    File
    Kafka
    Performance(记录一段时间内处理的记录数)
    Stdout

      