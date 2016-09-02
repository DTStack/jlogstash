
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
    IpIp
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

      
