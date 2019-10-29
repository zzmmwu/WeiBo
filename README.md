# WeiBo
一个高性能微博/朋友圈/空间类系统架构，支持千万活跃、百万在线、十万QPS。服务集群支持在线缩扩容、熔断，支持远程日志、统一监控。
本框架主体采用golang+grpc实现。

## 目录结构

├─common

│  ├─authToken		空

│  ├─dbConnPool		数据库连接池

│  ├─grpcStatsHandler	

│  ├─protobuf			整个框架的protobuf定义都在此

│  ├─rlog				远程日志和监控数据打点系统的接口定义

│  └─scaleAndBreak	可online线上缩扩容和自动熔断的服务管理

├─config				所有服务的配置文件

├─contentSvr			微博内容提取服务

├─dbTools

│  └─createDB		生成WeiBo框架mongodb数据库

├─followedSvr		用户粉丝数据提取服务（名字没有起好，叫follower更好）

├─followSvr			用户关注数据提取服务

├─frontNotifySvr		前端在线推送接入服务

├─frontNotifySvrMng	frontNotifySvr的管理

├─frontSvr			前端接入服务

├─frontSvrMng		frontSvr的管理

├─msgIdGenerator		全系统所有微博id生成器

├─postSvr			发博

├─pullSvr			拉取

├─pushSvr			在线推送

├─relationChangeSvr	关注（follow）和取关（unfollow）的关系处理

├─rlogSvr			远程日志和监控数据打点服务

├─statMonitor			

│  ├─h5Monitor		监控数据H5页面

│  └─monitorWsSvr	监控数据查看服务

├─test

│  ├─performTest		性能测试工具

│  ├─performTestDataGen	性能测试数据生成

│  │  ├─msgContentGen

│  │  └─relationGen

│  └─testClient		功能测试工具

└─usrMsgIdSvr		用户微博数据服务

