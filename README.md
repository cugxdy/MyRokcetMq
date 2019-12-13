# MyRocketMq

#### 介绍
使用java NIO技术作为消息存储引擎、使用java netty作为底层通信模块

#### 软件架构
1、基于reactor模式的实现, 利用java nio中selector对象
详情请看pdf: https://cugxdy.oss-cn-beijing.aliyuncs.com/java/nio.pdf<br/>
2、配置参数 : <br/>
&emsp;1、enableConsumeQueueExt : 决定是否启用TransientStorePool对象, default = false<br/>
&emsp;2、useReentrantLockWhenPutMessage: 决定是否启用ReentrantLock锁<br/>
&emsp;3、filterSupportRetry : 记录在Broker服务器过滤时, 是否使用ExpressionForRetryMessageFilter对象, 对RetryGroup主题过滤时,使用originalTopic对象<br/>
&emsp;4、enablePropertyFilter : 记录Broker服务器是否允许SQL92模式的过滤<br/>
&emsp;5、transientStorePoolEnable : 记录Broker服务器是否启用TransientStorePool对象 <br/>
3、RocketMq消息交互流程图:
![Image text](https://cugxdy.oss-cn-beijing.aliyuncs.com/picture/RocketMq%E4%BA%A4%E4%BA%92%E5%9B%BE.png)<br/>
4、MappFile对象提交与刷新流程图:
![Image text](https://cugxdy.oss-cn-beijing.aliyuncs.com/picture/ONUO5%28PY3_%7B%7D1DSK%25M62G47.png)<br/>
5、TopicConfigManager对象:
![Image text](https://cugxdy.oss-cn-beijing.aliyuncs.com/picture/9M%7B%7EXMBH5LC%7DL3SDB%2830_AR.png)<br/>
#### 安装教程
 
1. xxxx
2. xxxx
3. xxxx

#### 使用说明

1. xxxx
2. xxxx
3. xxxx

#### 参与贡献

1. Fork 本仓库
2. 新建 Feat_xxx 分支
3. 提交代码
4. 新建 Pull Request
