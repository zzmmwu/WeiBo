# WeiBo
一个高性能微博/朋友圈/空间类系统架构，支持千万活跃、百万在线、十万QPS。服务集群支持在线缩扩容、熔断，支持远程日志、统一监控。
本框架主体采用golang+grpc实现。

## 目录结构

├─common

│  ├─authToken		空

│  ├─dbConnPool		数据库连接池

│  ├─dbSvrConnPool		dbSvr连接池

│  ├─grpcStatsHandler	

│  ├─protobuf			整个框架的protobuf定义都在此

│  ├─rlog				远程日志和监控数据打点系统的接口定义

│  └─scaleAndBreak	可online线上缩扩容和自动熔断的服务管理

├─config				所有服务的配置文件

├─contentSvr			微博内容提取服务

├─dbSvr			      db统一接入服务

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


## 整体架构图

![整体架构图](https://wxgate01.5maogame.com/weibo/WeiBoFramework.png)


## frontSvrMng简述
frontSvr是面向客户端的接入服务。frontSvr不是固定url，每次客户端登录都动态分配。frontSvrMng就是面向客户端对于frontSvr的分配服务。客户端在接入weibo服务时，只需要知道frontSvrMng的url，通过查询命令即可得到本次登录分配给自己的frontSvr的url。

frontSvrMng支持对frontSvr的热缩扩容，实时熔断，而负荷分担则直接采用简单的循环分配即可。这部分能力可以参照common/scaleAndBreak包说明。

frontSvrMng的查询量和连接数不会太大（每次客户端连接微博服务时用短连接查询一次即可，直到断开连接后下次需要时再查。）我在性能测试时直接只用一台服务器就行了，实际生产环境用主从热备的两个url应该就ok了。如果登录量实在密集，比如每秒过十万级，那么可以用dns解析到多个url上。或者客户端上保存多个url进行循环使用，平时多url全部解析到一个地址，忙时分散。frontSvr对frontSvrMng没有向上依赖关系，所以一个和n个mng对frontSvr是透明的。

## frontSvr简述
frontSvr是客户端进行业务命令接入服务。采用多服务器平行扩展的方式进行部署（由frontSvrMng面向客户端进行统一管理）。

frontSvr通过grpc+protobuf接入。目前的实现中没有做鉴权和加密，实际商用中肯定是需要的，不过这部分比较独立这里就暂时不做了（太忙，后续有时间再加）。


服务架构如下
![整体架构图](https://wxgate01.5maogame.com/weibo/frontSvrFramework.png)

frontSvr将所有客户端的请求全部汇聚到10个req管道中，且每一个grpc请求都等待在自己对应的一个rsp接收管道上。每个req管道后面都对应一组routine负责req的发送、rsp的接收以及根据入管道时生成的reqId找到对应的rsp管道。

reqCmd routine从reqChan中取出reqCmd，然后根据cmd发送给对应的svr。frontSvr通过grpc双向stream连接了pullSvr/postSvr/relationChangeSvr，分别对应pull/post和follow/unfollow命令。

reqCmd routine负责根据配置文件连接对应的服务。所有服务连接支持平行扩展负荷分担，支持热扩容。支持实时检测可用性，如果一个svr不可用则马上尝试其他平行svr，直到成功或所有svr都不可用。

这里有同学可能会问，go语言并发能力那么强大，干嘛不在前端的grpcSvr routine中直接调用后端的服务，还要整些汇聚的管道呢？能简单处理的事情为什么要弄这么复杂。我的回答是前端请求必须汇聚后再传到后端服务的原因至少有这么几个吧：

1.  避震。客户端很可能存在并发量的剧烈震荡。如果让每一个客户端请求都直接连接和请求后端，那么这些剧烈震荡将是全系统的，很可能某些服务在某一次震荡中就被搞趴窝了。有了汇聚层，后端所有服务都会以自己能力来主动取请求去尽力处理，而不会被硬塞。每一层的服务都尽能力处理，这是整个系统最佳的设计（在这个weibo框架中其实也还没有做到，并没有在每一层服务都做吞吐汇聚。如果要更加健壮更加可靠的话，可以将每一层服务都做成这样的吞吐模式）。
2.  降低后端并发连接。一个客户端就会对应一个grpc连接，如果直接去连接和请求后端服务那么假如高峰期20台frontSvr，一个上面10万并发，那总共就会有200万并发连接。想象一个每一个连接都直接连接和请求后端服务，那么后端每一个服务器都要同时接200万并发，可能每台服务器都需要几百核几百G的配置了。
3.  防攻击。全部服务直连，一次DOS攻击可以把后端服务全部搞趴，而不仅仅只是部分接入服务了。

所以，直连的方式可以适合小规模且安全的网络环境，但不适合大并发的开放网络环境。

## pullSvr简述
pullSvr负责处理pull请求。他接受来之frontSvr的pull命令，然后根据用户id和lastMsgId去查询该用户关注的所有人中比lastMsgId更早一些的若干条微博消息（按照时间倒序）。

处理流程大致描述一下。pullSvr拿到pull请求后首先需要从followSvr查询到该用户的关注列表。然后去usrMsgIdSvr查询所有这些用户比lastMsgId更早一些的若干条消息id。汇总后按照时间倒序再选择出若干条，然后再去contentSvr查询这些消息的具体内容。最后返回给frontSvr。

  

服务架构：
![整体架构图](https://wxgate01.5maogame.com/weibo/pullSvrFramework.png)
pullSvr的前端是连接frontSvr的各routine。这些routine会再创建一个负责接收本连接所有rsp的管道并对应一个负责处理rsp的routine。通过一个req 管道将所有pullSvr的请求都汇聚起来。这条集中的req管道后面是若干处理routine（pullAssembleRoutine）。这些处理routine与followSvr/usrMsgSvr/contentSvr保持长连接，从req管道获取请求后按照流程处理。处理完后将rsp丢进对应的rsp的管道。

pullAssembleRoutine负责根据配置文件连接对应的服务。所有服务连接支持平行扩展负荷分担，支持热扩容。支持实时检测可用性，如果一个svr不可用则马上尝试其他平行svr，直到成功或所有svr都不可用。

pullAssembleRoutine的数量需要根据实际运行情况进行调整（我这里的测试用的100）。pullSvr的吞吐能力还取决于后端followSvr/usrMsgSvr/contentSvr。由于pullSvr是采用直连后端的模式，所以pullAssembleRoutine数量越大，后端的并发压力就越大；数量越小则pullSvr的并发处理能力会减弱。假设一个完整的pull流程会花费20ms（如果没有cache命中需要db加载则更长），那么100个routine的极限吞吐就是100*1/0.02=5000。更加科学和健壮的设计可以不用这种直连模式，所有后端也采用请求汇聚、按能力处理的方式。

pullSvr支持平行扩展，热缩扩容，故障熔断。这些能力都由frontSvr中的连接模块实现。（这里的实现没有用统一接入的透明连接模式，因为是所有svr都是内部开发嘛所以就由连接模块来实现了）

## followSvr简述
followSvr负责查询用户的关注列表。

  

服务架构：
![整体架构图](https://wxgate01.5maogame.com/weibo/followSvrFramework.png)
followSvr采用直连模式，未做请求队列汇聚。并发上限为pullSvr数量乘上pullSvr中pullAssembleRoutine的数量加上relationChangeSvr数量乘上relationChangeSvr中procRoutine的数量。

目前followSvr只做了单台部署设计，未做负荷分担等设计。主要是按照设计的量基本够用，还有更重要的原因是精力和时间有限。

## usrMsgIdSvr简述
usrMsgIdSvr负责根据用户id和lastMsgId查询用户的在lastMsgId之前的历史微博id。这里不查询内容，只查询id。支持单个和批量用户查询。一次批量查询只会返回所有usrid中时间最近的若干条消息的id。

  

服务架构：
![整体架构图](https://wxgate01.5maogame.com/weibo/usrMsgIdSvrFramework.png)
usrMsgIdSvr采用直连模式，未做请求队列汇聚。并发上限为pullSvr数量乘上pullSvr中pullAssembleRoutine的数量加上postSvr数量乘上postRoutine数量。

usrMsgIdSvr采用userId分片+平行扩展的部署模式。并支持热缩扩容，故障熔断。这些能力都由pullSvr和postSvr中的连接模块实现。（这里的实现没有用统一接入的透明连接模式，因为是所有svr都是内部开发嘛所以就由连接模块来实现了）
![整体架构图](https://wxgate01.5maogame.com/weibo/usrMsgIdSvrShard.png)

## contentSvr简述
usrMsgIdSvr负责根据msgid获取微博消息的具体内容。支持单个和批量查询。

  

服务架构：
![整体架构图](https://wxgate01.5maogame.com/weibo/contentSvrFramework.png)
contentSvr采用直连模式，未做请求队列汇聚。并发上限为pullSvr数量乘上pullSvr中pullAssembleRoutine的数量加上postSvr数量乘上postRoutine数量。

  

contentSvr采用msgId分片+平行扩展的部署模式。并支持热缩扩容，故障熔断。这些能力都由pullSvr和postSvr中的连接模块实现。（这里的实现没有用统一接入的透明连接模式，因为是所有svr都是内部开发嘛所以就由连接模块来实现了）
![整体架构图](https://wxgate01.5maogame.com/weibo/contentSvrShard.png)

## relationChangeSvr简述
relationChangeSvr负责处理关注和取关命令。负责刷新数据库的关系表和实时通知followSvr/followedSvr。

  

服务架构：
![整体架构图](https://wxgate01.5maogame.com/weibo/relationChangeSvrFramework.png)
relationChangeSvr的前端是连接frontSvr的各routine。这些routine会再创建一个负责接收本连接所有rsp的管道并对应一个负责处理rsp的routine。通过一个req 管道将所有请求都汇聚起来。这条集中的req管道后面是若干处理routine（procRoutine）。这些处理routine与followSvr/folledSvr/DB保持长连接，从req管道获取请求后按照流程处理。处理完后将rsp丢进对应的rsp的管道。

procRoutine的数量需要根据实际运行情况进行调整（我这里的测试用的100）。pullSvr的吞吐能力主要还取决于后端DB。procRoutine数量越大，DB的并发压力就越大；数量越小则relationChangeSvr的并发处理能力会减弱。假设一个完整的follow/unfollow流程会花费20ms，那么100个routine的极限吞吐就是100*1/0.02=5000。如果DB分片处理能力够强则可以加大并发量，反之可以减小。

relationChangeSvr支持平行扩展，热缩扩容，故障熔断。这些能力都由frontSvr中的连接模块实现。（这里的实现没有用统一接入的透明连接模式，因为是所有svr都是内部开发嘛所以就由连接模块来实现了）

## followedSvr简述
followedSvr负责查询用户的粉丝列表。

  

服务架构：
![整体架构图](https://wxgate01.5maogame.com/weibo/followedSvrFramework.png)
followedSvr采用直连模式，未做请求队列汇聚。并发上限为pushSvr数量乘上pushSvr中pushRoutine的数量加上relationChangeSvr数量乘上relationChangeSvr中procRoutine的数量。

followedSvr上除了缓存粉丝列表外，还缓存了在线用户表。目的是为了降低通信量。微博的一个大V的粉丝数就有几千万，在本次设计的数量量下大V也是百万粉丝级别。而在线的粉丝至少会低一个数量级。在线推送则只需要获取在线粉丝列表即可，所以在此缓存了一份实时在线用户表，作为过滤。

目前followedSvr只做了单台部署设计，未做负荷分担等设计。主要是按照设计的量基本够用，还有更重要的原因是精力和时间有限。

## postSvr简述
postSvr负责处理post请求（发微博）。负责入DB，通知刷新usrMsgIdSvr，并且通知pushSvr对在线粉丝发送通知。

  

服务架构：
![整体架构图](https://wxgate01.5maogame.com/weibo/postSvrFramework.png)
postSvr的前端是连接frontSvr的各routine。这些routine会再创建一个负责接收本连接所有rsp的管道并对应一个负责处理rsp的routine。通过一个req 管道将所有pullSvr的请求都汇聚起来。这条集中的req管道后面是若干处理routine（postRoutine）。这些处理routine与usrMsgIdSvr/DB/pushSvr保持长连接，从req管道获取请求后按照流程处理。处理完后将rsp丢进对应的rsp的管道。

msgIdGnerator接受多个并发postSvr的请求，互斥的生成不重复的msgId，严格保证在全系统中msgId按照时间顺序递增。在pull操作时，无需比较时间戳，直接用msgId就能进行时间排序。

postSvr不会去连接contentSvr，contentSvr中的缓存由粉丝的pull操作来驱动。

postRoutine负责根据配置文件连接对应的服务。所有服务连接支持平行扩展负荷分担，支持热扩容。支持实时检测可用性，如果一个svr不可用则马上尝试其他平行svr，直到成功或所有svr都不可用。

postRoutine的数量需要根据实际运行情况进行调整（我这里的测试用的100）。由于实际的weibo系统中post的量是比较小的（相对与pull）所以这里基本不会是什么问题。post引发的对粉丝的push操作会是个考验，不过这个是pushSvr干的活了。这里只是简单的发送一条通知跟pushSvr就完事。

postSvr支持平行扩展，热缩扩容，故障熔断。这些能力都由frontSvr中的连接模块实现。（这里的实现没有用统一接入的透明连接模式，因为是所有svr都是内部开发嘛所以就由连接模块来实现了）

## msgIdGenerator简述
msgIdGnerator接受多个并发postSvr的请求，互斥的生成不重复的msgId，严格保证在全系统中msgId按照时间顺序递增。msgId的顺序即是微博的时间顺序。

## pushSvr简述
pushSvr负责在用户post一条微博后向其所有在线粉丝发送通知。

  

服务架构：
![整体架构图](https://wxgate01.5maogame.com/weibo/pushSvrFramework.png)
pushSvr中有一个userOnlineCache，这个cache除了缓存在线用户id还保存了用户对应的frontNotifySvr。

pushSvr从postSvr接受push命令。然后会将所有push命令通过一个req 管道汇聚起来。这条集中的req管道后面是若干处理routine（pRoutine）。

这些处理pushRoutine与followedSvr保持长连接，从req管道获取请求后会先向followedSvr查询在线的粉丝。然后再根据userOnlineCache中的数据将所有粉丝按照frontNotifySvr进行分组。分组完成后再批量向frontNotifySvr发送noitfy。

pushSvr支持平行扩展，热缩扩容，故障熔断。这些能力都由postSvr和frontNotifySvr中的连接模块实现。（这里的实现没有用统一接入的透明连接模式，因为是所有svr都是内部开发嘛所以就由连接模块来实现了）

pushSvr只负责在线用户的通知，因为微博系统中post操作相对与pull操作有数量级的差距，并且在线粉丝数量一般都与粉丝数差一个数量级。所以pushSvr只有在遇到大V发帖时才会有压力。在性能测试的数据模型中，大V粉丝数量在百万级别，在线粉丝大概在十万，目前的架构基本不会有性能压力。即使真实的微博，千万粉丝级的头部大V发帖，其在线粉丝数也基本在百万级别，一个pushRoutine也能在1~2秒内处理完并转发给所有frontNotifySvr。

  

这里并未实现离线粉丝的推送功能，这个功能是另一个话题与另外一个系统了。离线粉丝这里会真正考验推送系统的能力。目前Twitter的要求好像是5千万级别的头部大V要在5秒内完成全球范围内的粉丝推送。

## frontNotifySvr简述
frontNotifySvr是客户端进行业务命令接入服务。采用多服务器平行扩展的方式进行部署（由frontNotifySvrMng面向客户端进行统一管理）。

frontNotifySvr通过grpc+protobuf接入。目前的实现中没有做鉴权和加密，实际商用中肯定是需要的，不过这部分比较独立这里就暂时不做了（太忙，后续有时间再加）。

  

服务架构如下
![整体架构图](https://wxgate01.5maogame.com/weibo/frontNotifySvrFramework.png)
客户端通过单向stream连接frontNotifySvr来接收notify，并通过心跳来维持在线状态。客户端创建notifyStream即表示登录在线，grpc连接断开或心跳超时则下线。

所有online/offline命令全部汇聚到reqChan管道中，由onlineProcRoutine负责向pushSvr和followedSvr转发。

所有本svr服务的用户的在线信息缓存在UserIdOnlineDataMap中，其中包含了userId对应的通知接收管道notifyChan。当notifyRecvRoutine从pushSvr接收到noitfy消息后就会在此缓存中用userid查找对应的notifyChan，然后插入notify消息。

## frontNotifySvrMng简述
frontNotifySvr是面向客户端的通知推送服务。frontNotifySvr不是固定url，每次客户端登录都动态分配。frontNotifySvrMng就是面向客户端对于frontNotifySvr的分配服务。客户端在接入weibo服务时，只需要知道frontNotifySvrMng的url，通过查询命令即可得到本次登录分配给自己的frontNotifySvr的url。

frontNotifySvrMng支持对frontNotifySvr的热缩扩容，实时熔断，而负荷分担则直接采用简单的循环分配即可。这部分能力可以参照common/scaleAndBreak包说明。

frontNotifySvrMng的查询量和连接数不会太大（每次客户端连接微博服务时用短连接查询一次即可，直到断开连接后下次需要时再查。）我在性能测试时直接只用一台服务器就行了，实际生产环境用主从热备的两个url应该就ok了。如果登录量实在密集，比如每秒过十万级，那么可以用dns解析到多个url上。或者客户端上保存多个url进行循环使用，平时多url全部解析到一个地址，忙时分散。frontNotifySvr对frontNotifySvrMng没有向上依赖关系，所以一个和n个mng对frontNotifySvr是透明的。

## dbSvr简述
dbSvr作为整个系统数据库的接入服务。所有数据库操作全部发送到dbSvr，由dbSvr集中处理。这样可以统一操作，让db设计对业务系统透明，也能集中进行db性能优化。

  

服务架构：
![整体架构图](https://wxgate01.5maogame.com/weibo/dbSvrFramework.png)
各svr通过grpc与dbSvr通信。dbSvr针对每一个db维护一条req管道。管道后是若干处理routine。这样保证各db的命令排队分割开来。每个db都按照自身能力来提供服务。

DB可采用分片的方式，尽量将各DB的压力均衡开。

## rlogSvr简述
rlogSvr是一个日志和状态数据远程集中式服务。系统中的服务通过common/rlog中的接口输出日志、告警和状态数据打点信息，统一发送到rlogSvr。便于集中日志查看和状态监控。

rlog提供如下接口：

-   Printf
-   Fatalf
-   Panicf
-   Warning
-   StatPoint

其中Printf/Fatalf/Panicf/Waring的接口除了会发送信息给rlogSvr，还会在本地通过log包输出。

## statMonitor简述
rlogSvr会将实时的状态打点数据刷新到数据库中，monitorWsSvr定时读取并提供webSocket服务定时向前端刷新。monitorWsSvr是用php workMan来写的，一个很简单的实现。

h5Monitor是一个用h5引擎做的简陋的状态打点数据监控页面，提供数据警示（标黄）和数据告警（标红）。非常简陋但还能用：）。实际生产环境可以用更专业的监控系统。

## scaleAndBreak包简述
scaleAndBreak包提供frontSvrMng/frontNotifySvrMng服务的所需要的对管理svr的平行扩展、在线缩扩容、可用性检测与故障熔断能力。

我们可以通过直接修改配置文件来随时增删所管理的服务器url，服务会每秒定时读取配置文件来刷新数据，实现在线热扩容。

所有被管理的svr都需要提供一个可用性检测的接口供调用，接口的实现由svr决定。通过该接口可以定时检测该svr的可用性，一旦发现问题即从可用svr列表中删掉实现故障熔断。并且在下次刷新时再次检测，如果发现之前熔断的svr变得可用，则加入到可用svr列表中。

## grpcStatHandler包简述
grpcStatHandler包提供grpc连接的统计和回调。便于数据统计以及在线状态维护。

## dbConnPool包简述
dbConnPool包提供数据库连接池服务。创建固定数量的数据库连接供循环使用。避免所有并发都去自己连接数据库，造成数据库连接数膨胀。

## dbSvrConnPool包简述
dbSvrConnPool包提供dbSvr连接池服务。创建固定数量的dbSvr连接供循环使用。避免所有并发都去自己连接dbSvr，造成dbSvr连接数膨胀。

## DB设计

这里专门介绍一下我针对weibo系统的数据库设计。

weibo系统只实现了最基本的pull/post/follow/unfollow命令，所以表也只有少数几个。

-   Follow表，存储用户的关注列表。字段有userid和followid。按照userid进行分为300张表，userid和followid作为联合index。
-   UserLevel表，存储用户等级。100粉以下为0级，1万粉以下为1级，10万粉以下为2级，100万粉以下为3级，100万粉以上为4级。字段有userid，level，followerCount，和其他字段（后面详细介绍）。按照userid分20张表。userid为index。
-   Followed表，存储用户粉丝表。字段有userid和followedid（粉丝id）。分表规则按照UserLevel进行。0~3级用户分别单独分表，每级100张。4级用户每个用户单独一张表。
-   UserMsgId表，存储用户发帖的id。字段有userid和msgid。按照userid分表，userid和msgid作为联合index。
-   ContentMsg表，存储微博消息。字段有msgid和其他内容信息。按照msgid分为100张表，msgid为index。

  

这里需要详细说明的userlevel表和Followed。因为用户粉丝数差距实在是太大，所以不能一股脑全部同样对待。这里就按照分数数进行分级，然后按等级分别对待。

但考虑到用户等级是可能上升的，怎么办呢？这里就在userlevel表中增加了intrans和transbegintime字段。每晚可以定期用userLevelTrans程序扫描level表，查看粉丝数。如果达到升级标准，则将intrans字段和transBeginTime置位，然后进行数据迁移。dbSvr在follow和unfollow时如果看到intrans字段置位，则不再修改对应的followed表，而将数据写入TransFollowed和TransUnFollowed表中。userLevelTrans迁移数据完成后，修改level和intrans字段，再将TransFollowed和TransUnFollowed表中记录的数据转入Followed表。ok





