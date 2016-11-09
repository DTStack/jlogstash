用java版本重写logstash目的是提升性能,跟ruby logstash 对比 请看 https://github.com/DTStack/jlogstash-performance-testing

不同的业务，性能会有所不同，dtstack 后台日志解析 java 版本是ruby版本的5倍，在单台4g 4cores 虚拟机上。

jlogstash 的参数配置和使用看wiki介绍，现在的插件跟ruby版本相比还太少，希望更多的人参与开发。

各个插件代码在jlogstash-input-plugin，jlogstash-output-plugin，jlogstash-filter-plugin。

