/*
@Author : Ryan.wuxiaoyong
*/

package main

import (
	"context"
	"encoding/json"
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"io"
	"sync/atomic"

	//"go/types"
	"io/ioutil"
	//"log"
	"net"
	"os"
	"sync"
	"time"
	"fmt"

	"WeiBo/common"
	"WeiBo/common/grpcStatsHandler"
	pb "WeiBo/common/protobuf"
	"WeiBo/common/rlog"
)

//配置文件格式
type configType struct {
	MyName string `json: "myName"`
	RlogSvrAddr string `json: "rlogSvrAddr"`
	ListenAddr string  `json: "listenAddr"`
	RelationChgSvrAddr string `json: "relationChgSvrAddr"`
}
//配置文件数据对象
var gConfig = &configType{}

type shareSvrArrConfigT struct {
	PullSvrAddrArr []string `json: "pullSvrAddrArr"`
	PostSvrAddrArr []string `json: "postSvrAddrArr"`
}

//pullSvr负荷分担数据/////////////////////////////////////////
type pullSvrShareT struct {
	sync.RWMutex

	SvrAddrArr []string //平行负荷分担，轮询
	curIndex int //当前轮询index
}
//
var gPullSvrData = pullSvrShareT{}
//postSvr负荷分担数据/////////////////////////////////////////
type postSvrShareT struct {
	sync.RWMutex

	SvrAddrArr []string //平行负荷分担，轮询
	curIndex int //当前轮询index
}
//
var gPostSvrData = postSvrShareT{}


//接受rsp的管道map，reqId为key
type rspChanMapT struct {
	sync.RWMutex

	reqId2ChnMap map[int64]chan *common.RspCmdT
}
func (m *rspChanMapT)getChn(reqId int64) (chan *common.RspCmdT, bool) {
	m.RLock()
	defer m.RUnlock()
	rsp, ok := m.reqId2ChnMap[reqId]
	return rsp, ok
}
func (m *rspChanMapT)insertChn(reqId int64, chn chan *common.RspCmdT) {
	m.Lock()
	defer m.Unlock()
	m.reqId2ChnMap[reqId] = chn
}
func (m *rspChanMapT)delChn(reqId int64) {
	m.Lock()
	defer m.Unlock()
	delete(m.reqId2ChnMap, reqId)
}

//
var gRspChanMap = rspChanMapT{sync.RWMutex{}, map[int64]chan *common.RspCmdT{}}


//接受req的管道结构，所有连接的请求都汇聚到这里的某条管道，并由管道后的roution进行处理
var gReqChanArr []chan *common.ReqCmdT

//req的计数器，为线程安全请用atomic.AddUint64()进行操作
var gReqPullCounter int64
var gReqPostCounter int64
var gReqFollowCounter int64
var gReqUnFollowCounter int64
//rsp计数器，为线程安全请用atomic.AddUint64()进行操作
var gRspPullCounter int64
var gRspPostCounter int64
var gRspFollowCounter int64
var gRspUnFollowCounter int64
//客户端连接统计
var gConnCounterHandle = grpcStatsHandler.ConnCounterHandler{}

//req编号，保证本机唯一
type reqIdGenT struct {
	sync.Mutex

	lastReqId int64
}
func (g *reqIdGenT)getOneId() int64{
	g.Lock()
	defer g.Unlock()
	g.lastReqId++
	return g.lastReqId
}
//
var gReqIdGen = reqIdGenT{sync.Mutex{}, 0}

func main(){
	if len(os.Args)!=2 {
		rlog.Fatalf("xxx configPath")
	}
	confPath := os.Args[1]

	//
	confBytes, err := ioutil.ReadFile(confPath)
	if err != nil {
		rlog.Fatalf("Read config file failed.[%s][%+v]", confPath, err)
	}
	//解析
	err = json.Unmarshal(confBytes, gConfig)
	if err != nil {
		rlog.Fatalf("Read config file failed.[%s][%+v]", confPath, err)
	}

	rlog.Init(gConfig.MyName, gConfig.RlogSvrAddr)

	//启动配置文件刷新routine
	go startConfigRefresh(confPath)

	//启动n个命令管道和routine负责处理所有连接的请求
	for i:=0; i<10; i++{
		chn := make(chan *common.ReqCmdT, 1000)
		gReqChanArr = append(gReqChanArr, chn)

		////设置为needRefresh，实际发生请求时会先向mngSvr询查一个地址
		//pullSvrInfo := pullSvrInfoT{sync.Mutex{}, true, ""}
		////
		//postSvrInfo := postSvrInfoT{sync.Mutex{}, true, ""}

		go reqCmdRoutine(chn)
	}

	//开启数据打点routine
	go statReportRoutine()

	//开启grpc服务
	lis, err := net.Listen("tcp", gConfig.ListenAddr)
	if err != nil {
		rlog.Fatalf("failed to listen: %+v", err)
	}
	rlog.Printf("begin to Listen [%s]", gConfig.ListenAddr)
	//客户端idle超时设置
	svrOpt := grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionIdle: (common.ClientMaxIdleSec+5)*time.Second})
	handler := grpc.StatsHandler(&gConnCounterHandle)
	grpcServer := grpc.NewServer(svrOpt, handler)
	pb.RegisterFrontSvrServer(grpcServer, &serverT{})
	if err := grpcServer.Serve(lis); err != nil {
		rlog.Fatalf("failed to serve: %+v", err)
	}

}

func statReportRoutine(){
	for {
		//x秒上报一次
		time.Sleep(5*time.Second)

		var dataArr []rlog.StatPointData
		{
			data := rlog.StatPointData{Name:"connCount", Data:int64(gConnCounterHandle.GetConnCount())}
			dataArr = append(dataArr, data)

			data = rlog.StatPointData{Name:"reqPullCount", Data:gReqPullCounter}
			dataArr = append(dataArr, data)
			data = rlog.StatPointData{Name:"reqPostCount", Data:gReqPostCounter}
			dataArr = append(dataArr, data)
			data = rlog.StatPointData{Name:"reqFollowCount", Data:gReqFollowCounter}
			dataArr = append(dataArr, data)
			data = rlog.StatPointData{Name:"reqUnFollowCount", Data:gReqUnFollowCounter}
			dataArr = append(dataArr, data)

			data = rlog.StatPointData{Name:"rspPullCount", Data:gRspPullCounter}
			dataArr = append(dataArr, data)
			data = rlog.StatPointData{Name:"rspPostCount", Data:gRspPostCounter}
			dataArr = append(dataArr, data)
			data = rlog.StatPointData{Name:"rspFollowCount", Data:gRspFollowCounter}
			dataArr = append(dataArr, data)
			data = rlog.StatPointData{Name:"rspUnFollowCount", Data:gRspUnFollowCounter}
			dataArr = append(dataArr, data)

			for i := range gReqChanArr{
				data = rlog.StatPointData{Name:fmt.Sprintf("reqChanLen_%d", i), Data:int64(len(gReqChanArr[i]))}
				dataArr = append(dataArr, data)
			}

		}
		rlog.StatPoint("totalReport", dataArr)
	}
}

//
////pullSvr集群是否发生改变，如果改变则需要从新连接新的pullSvr
//type pullSvrInfoT struct {
//	sync.Mutex
//	needRefresh bool
//	addr string
//}
//func (info *pullSvrInfoT)getInfoAndClear()(bool, string){
//	info.Lock()
//	defer info.Unlock()
//	need := info.needRefresh
//	info.needRefresh = false
//	return need, info.addr
//}
//func (info *pullSvrInfoT)setInfo(needRefresh bool, addr string){
//	info.Lock()
//	defer info.Unlock()
//	info.needRefresh = needRefresh
//	info.addr = addr
//}
//func (info *pullSvrInfoT)setNeedRefresh(){
//	info.Lock()
//	defer info.Unlock()
//	info.needRefresh = true
//}
//
//// 如果需要重连，则从新开启一个连接。得到新的ClientConn和PullSvr_CreateStreamClient，链接失败返回ClientConn为nil。
//// 有重连的时候，旧的PullSvr_CreateStreamClient会被CloseSend
//func reconnPullSvrIfNeed(pullSvrInfo *pullSvrInfoT, clientStream pb.PullSvr_CreateStreamClient) (*grpc.ClientConn, pb.PullSvr_CreateStreamClient){
//	//if clientStream == nil{
//	//	log.Printf("clientStream==nil")
//	//}
//	needRefresh, newAddr := pullSvrInfo.getInfoAndClear()
//	if !needRefresh {
//		//log.Printf("no needRefresh")
//		return nil, clientStream
//	}
//
//	conn, err := grpc.Dial(newAddr, grpc.WithInsecure())
//	if err != nil {
//		log.Printf("[reconnPullSvrIfNeed] connect pullSvr failed. [%+v]", err)
//		return nil, clientStream
//	}
//	client := pb.NewPullSvrClient(conn)
//	stream, err := client.CreateStream(context.Background())
//	if err != nil {
//		log.Printf("[reconnPullSvrIfNeed] CreateStream failed. [%+v]", err)
//		return nil, clientStream
//	}
//	//if stream == nil{
//	//	log.Printf("stream==nil")
//	//}
//	//log.Printf("new connection")
//	return conn, stream
//}
//func pullSvrInfoRefreshRoutine(pullMngAddr string, pullSvrInfo *pullSvrInfoT){
//	var lastRefreshTime int64 = 0
//
//	for {
//		conn, err := grpc.Dial(pullMngAddr, grpc.WithInsecure())
//		if err != nil {
//			log.Printf("connect pullMng failed. [%+v]", err)
//		}else{
//			client := pb.NewPullSvrMngClient(conn)
//			//获取Svr地址
//			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//			rsp, err := client.QuerySvrAddr(ctx, &pb.QuerySvrAddrReq{LastRefreshTime:lastRefreshTime})
//			cancel()
//			if err != nil{
//				log.Printf("QueryPullSvrAddr failed. [%+v]", err)
//			}else{
//				if rsp.NeedRefresh{
//					pullSvrInfo.setInfo(true, rsp.Addr)
//					lastRefreshTime = rsp.AnsTime
//				}
//			}
//
//			//
//			_ = conn.Close()
//		}
//
//		//
//		time.Sleep(1*time.Second)
//	}
//}
//pull rsp接收routine从stream接收包。如果接收到EOF则closeConnect
//对应的，pullSvr上如果30秒没有rsp了，且reqStream已经EOF则return nil
func pullSvrRecvRoutine(conn *grpc.ClientConn, clientStream pb.PullSvr_CreateStreamClient){
	for {
		rsp, err := clientStream.Recv()
		if err == io.EOF || err != nil{
			_ = conn.Close()
			rlog.Printf("pullSvrRecvRoutine. conn closed. err=[%+v]", err)
			return
		}
		if rsp.FrontReqId == 0{
			//空包，心跳包
			//log.Printf("postSvrRecvRoutine recv=[%+v]", rsp)
			continue
		}
		rspCmd := &common.RspCmdT{ReqId:rsp.FrontReqId, RspMsg:rsp.MsgArr}
		rspChn, ok := gRspChanMap.reqId2ChnMap[rspCmd.ReqId]
		if ok{
			rspChn <- rspCmd
		}else{
			rlog.Printf("rspChn not found")
		}
	}
	////test
	//cmd = &common.RspCmdT{rsp.reqId, []*pb.MsgData{&pb.MsgData{UserId: 12345, Text: "hello test text1", ImgUrlArr: []string{}, VideoUrl:""},
	//	&pb.MsgData{UserId: 23456, Text: "hello test text2", ImgUrlArr: []string{}, VideoUrl:""}}}

}

//
////pullSvr集群是否发生改变，如果改变则需要从新连接新的pullSvr
//type postSvrInfoT struct {
//	sync.Mutex
//	needRefresh bool
//	addr string
//}
//func (info *postSvrInfoT)getInfoAndClear()(bool, string){
//	info.Lock()
//	defer info.Unlock()
//	need := info.needRefresh
//	info.needRefresh = false
//	return need, info.addr
//}
//func (info *postSvrInfoT)setInfo(needRefresh bool, addr string){
//	info.Lock()
//	defer info.Unlock()
//	info.needRefresh = needRefresh
//	info.addr = addr
//}
//func (info *postSvrInfoT)setNeedRefresh(){
//	info.Lock()
//	defer info.Unlock()
//	info.needRefresh = true
//}
//// 如果需要重连，则从新开启一个连接。得到新的ClientConn和PostSvr_CreateStreamClient，链接失败返回ClientConn为nil。
//// 有重连的时候，旧的PullSvr_CreateStreamClient会被CloseSend
//func reconnPostSvrIfNeed(postSvrInfo *postSvrInfoT, clientStream pb.PostSvr_CreateStreamClient) (*grpc.ClientConn, pb.PostSvr_CreateStreamClient){
//	needRefresh, newAddr := postSvrInfo.getInfoAndClear()
//	if !needRefresh {
//		//log.Printf("no needRefresh")
//		return nil, clientStream
//	}
//
//	conn, err := grpc.Dial(newAddr, grpc.WithInsecure())
//	if err != nil {
//		log.Printf("[reconnPullSvrIfNeed] connect pullSvr failed. [%+v]", err)
//		return nil, clientStream
//	}
//	client := pb.NewPostSvrClient(conn)
//	stream, err := client.CreateStream(context.Background())
//	if err != nil {
//		log.Printf("[reconnPostSvrIfNeed] CreateStream failed. [%+v]", err)
//		return nil, clientStream
//	}
//	return conn, stream
//}
//func postSvrInfoRefreshRoutine(postMngAddr string, postSvrInfo *postSvrInfoT){
//	var lastRefreshTime int64 = 0
//
//	for {
//		conn, err := grpc.Dial(postMngAddr, grpc.WithInsecure())
//		if err != nil {
//			log.Printf("connect postMng failed. [%+v]", err)
//		}else{
//			client := pb.NewPostSvrMngClient(conn)
//			//获取Svr地址
//			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//			rsp, err := client.QuerySvrAddr(ctx, &pb.QuerySvrAddrReq{LastRefreshTime:lastRefreshTime})
//			cancel()
//			if err != nil{
//				log.Printf("QueryPostSvrAddr failed. [%+v]", err)
//			}else{
//				if rsp.NeedRefresh{
//					postSvrInfo.setInfo(true, rsp.Addr)
//					lastRefreshTime = rsp.AnsTime
//				}
//			}
//
//			//
//			_ = conn.Close()
//		}
//
//		//
//		time.Sleep(1*time.Second)
//	}
//}
//post rsp接收routine从stream接收包。如果接收到EOF则closeConnect
//对应的，postSvr上如果30秒没有rsp了，且reqStream已经EOF则return nil
func postSvrRecvRoutine(conn *grpc.ClientConn, clientStream pb.PostSvr_CreateStreamClient){
	for {
		rsp, err := clientStream.Recv()
		if err == io.EOF || err != nil{
			_ = conn.Close()
			rlog.Printf("pullSvrRecvRoutine. conn closed. err=[%+v]", err)
			return
		}
		if rsp.FrontReqId == 0{
			//空包，心跳包
			//log.Printf("postSvrRecvRoutine recv=[%+v]", rsp)
			continue
		}
		rspCmd := &common.RspCmdT{ReqId:rsp.FrontReqId, RspMsg:rsp}
		rspChn, ok := gRspChanMap.reqId2ChnMap[rspCmd.ReqId]
		if ok{
			rspChn <- rspCmd
		}else{
			rlog.Printf("rspChn not found")
		}
	}
	////test
	//cmd = &common.RspCmdT{rsp.reqId, []*pb.MsgData{&pb.MsgData{UserId: 12345, Text: "hello test text1", ImgUrlArr: []string{}, VideoUrl:""},
	//	&pb.MsgData{UserId: 23456, Text: "hello test text2", ImgUrlArr: []string{}, VideoUrl:""}}}

}

func reqCmdRoutine(chn chan *common.ReqCmdT) {
	//服务器地址到链接的map，给userIdMsgSvr和contentSvr的链接用
	addr2ConnMap := make(map[string]grpcConn)

	//stream保活定时器
	keepaliveTimer := time.NewTimer(1*time.Minute)

	for {
		select {
		case <-keepaliveTimer.C:
			keepaliveTimer.Reset(1*time.Minute)
			_ = sendReqToPullSvr(0, 0, 0, addr2ConnMap)
			_ = sendReqToPostSvr(0, nil, addr2ConnMap)
			break
		case reqCmd, ok := <- chn:
			if !ok{
				rlog.Panicf("reqCmdRoutine <- chn failed. impossible")
			}

			switch reqCmd.Cmd {
			case common.CmdPull:
				frontReq := reqCmd.ReqMsg.(*pb.CPullReq)
				err := sendReqToPullSvr(reqCmd.ReqId, frontReq.UserId, frontReq.LastMsgId, addr2ConnMap)
				if err != nil{
					rlog.Printf("sendReqToPullSvr failed. err=[%+v]", err)
					//丢弃
					continue
				}

				break
			case common.CmdPost:
				frontReq := reqCmd.ReqMsg.(*pb.CPostReq)
				err := sendReqToPostSvr(reqCmd.ReqId, frontReq.Msg, addr2ConnMap)
				if err != nil{
					rlog.Printf("sendReqToPostSvr failed. err=[%+v]", err)
					//丢弃
					continue
				}

				break
			case common.CmdFollow:
				break
			case common.CmdUnFollow:
				break
			case common.CmdLike:
				break
			default:
				rlog.Panicf("bad cmd[%d], impossible", reqCmd.Cmd)
			}
			break
		}

	}
}
//获取地址对应的链接，如果还未链接则链接
type grpcConn struct {
	Conn *grpc.ClientConn
	Client interface{}
	Stream interface{}
}
func sendReqToPullSvr(reqId int64, userId int64, lastMsgId int64, addr2ConnMap map[string]grpcConn) error{

	//先把svr地址拷贝出来
	gPullSvrData.Lock()
	//用值拷贝，防止引用用一个数组，gUserMsgIdSvrData中的数组是处于竞态环境
	svrAddrArr := make([]string, len(gPullSvrData.SvrAddrArr))
	copy(svrAddrArr, gPullSvrData.SvrAddrArr)
	startSvrIndex := gPullSvrData.curIndex
	gPullSvrData.Unlock()

	//
	curIndex := startSvrIndex-1
	for range svrAddrArr{
		curIndex++
		if curIndex >= len(svrAddrArr) {
			curIndex = 0
		}
		addr := svrAddrArr[curIndex]
		//允许重试一次
		for i:=0; i<2; i++ {
			//获取地址对应的链接
			var connData grpcConn
			{
				var ok bool
				connData, ok = addr2ConnMap[addr]
				if !ok {
					//还未链接则链接
					pullConn, err := grpc.Dial(addr, grpc.WithInsecure())
					if err != nil {
						rlog.Printf("connect pushSvr failed. addr=[%s] [%+v]", addr, err)
						//连不上，视为服务down掉，忽略
						break
					}
					client := pb.NewPullSvrClient(pullConn)
					pullStream, err := client.CreateStream(context.Background())
					if err != nil {
						rlog.Printf("[pull] CreateStream failed. [%+v]", err)
						//视为服务down掉，忽略
						break
					}
					//启动新的接受routine
					go pullSvrRecvRoutine(pullConn, pullStream)

					connData = grpcConn{Conn: pullConn, Client: client, Stream:pullStream}
					addr2ConnMap[addr] = connData
				}
			}
			err := connData.Stream.(pb.PullSvr_CreateStreamClient).Send(&pb.PullReq{FrontReqId: reqId, UserId:userId, LastMsgId:lastMsgId})
			if err != nil {
				rlog.Printf("pull failed. addr=[%s] [%+v] userId=%d", addr, err, userId)
				//断开连接
				_ = connData.Conn.Close()
				delete(addr2ConnMap, addr)
				//重试一次
				time.Sleep(100*time.Millisecond)//等100ms，只有断链后的第一次尝试才会sleep，所以不大会影响性能
				continue
			}

			//发送成功，向任意一个pushSvr发送成功即可
			return nil
		}
	}

	return errors.New("no pullSvr available")
}
func sendReqToPostSvr(reqId int64, content *pb.MsgData, addr2ConnMap map[string]grpcConn) error{

	//先把svr地址拷贝出来
	gPostSvrData.Lock()
	//用值拷贝，防止引用用一个数组，gUserMsgIdSvrData中的数组是处于竞态环境
	svrAddrArr := make([]string, len(gPostSvrData.SvrAddrArr))
	copy(svrAddrArr, gPostSvrData.SvrAddrArr)
	startSvrIndex := gPostSvrData.curIndex
	gPostSvrData.Unlock()

	//
	curIndex := startSvrIndex-1
	for range svrAddrArr{
		curIndex++
		if curIndex >= len(svrAddrArr) {
			curIndex = 0
		}
		addr := svrAddrArr[curIndex]
		//允许重试一次
		for i:=0; i<2; i++ {
			//获取地址对应的链接
			var connData grpcConn
			{
				var ok bool
				connData, ok = addr2ConnMap[addr]
				if !ok {
					//还未链接则链接
					pullConn, err := grpc.Dial(addr, grpc.WithInsecure())
					if err != nil {
						rlog.Printf("connect pushSvr failed. addr=[%s] [%+v]", addr, err)
						//连不上，视为服务down掉，忽略
						break
					}
					client := pb.NewPostSvrClient(pullConn)
					postStream, err := client.CreateStream(context.Background())
					if err != nil {
						rlog.Printf("[post] CreateStream failed. [%+v]", err)
						//视为服务down掉，忽略
						break
					}
					//启动新的接受routine
					go postSvrRecvRoutine(pullConn, postStream)

					connData = grpcConn{Conn: pullConn, Client: client, Stream:postStream}
					addr2ConnMap[addr] = connData
				}
			}
			err := connData.Stream.(pb.PostSvr_CreateStreamClient).Send(&pb.PostReq{FrontReqId: reqId, Content:content})
			if err != nil {
				rlog.Printf("post failed. addr=[%s] [%+v] content=%d", addr, err, content)
				//断开连接
				_ = connData.Conn.Close()
				delete(addr2ConnMap, addr)
				//重试一次
				time.Sleep(100*time.Millisecond)//等100ms，只有断链后的第一次尝试才会sleep，所以不大会影响性能
				continue
			}

			//发送成功，向任意一个pushSvr发送成功即可
			return nil
		}
	}

	return errors.New("no postSvr available")
}
//
//func reqCmdRoutine(chn chan *common.ReqCmdT, pullSvrInfo *pullSvrInfoT, postSvrInfo *postSvrInfoT) {
//	//启动pullSvr地址询问
//	go pullSvrInfoRefreshRoutine(gConfig.PullSvrMngAddr, pullSvrInfo)
//	//pullSvr Stream
//	var pullStream pb.PullSvr_CreateStreamClient = nil
//	//pullSvrConn
//
//	//启动postSvr地址询问
//	go postSvrInfoRefreshRoutine(gConfig.PostSvrMngAddr, postSvrInfo)
//	//postSvr Stream
//	var postStream pb.PostSvr_CreateStreamClient = nil
//
//	for {
//		reqCmd, ok := <- chn
//		if !ok{
//			log.Panicf("reqCmdRoutine <- chn failed. impossible")
//		}
//
//		switch reqCmd.Cmd {
//		case common.CmdPull:
//			var pullConn *grpc.ClientConn
//			pullConn, pullStream = reconnPullSvrIfNeed(pullSvrInfo, pullStream)
//			if pullConn != nil{
//				//新连接或发生了重连，重启一个接收routine
//				go pullSvrRecvRoutine(pullConn, pullStream)
//			}
//			frontReq := reqCmd.ReqMsg.(*pb.CPullReq)
//			//log.Printf("pullStream=[%+v]", pullStream)
//			//log.Printf("reqCmd=[%+v]", reqCmd)
//			//log.Printf("frontReq-[%+v]", frontReq)
//			err := pullStream.Send(&pb.PullReq{FrontReqId: reqCmd.ReqId, UserId:frontReq.UserId, LastMsgId:frontReq.LastMsgId})
//			if err != nil {
//				//标记重连，本次业务丢弃
//				pullSvrInfo.setNeedRefresh()
//			}
//
//			break
//		case common.CmdPost:
//			var postConn *grpc.ClientConn
//			postConn, postStream = reconnPostSvrIfNeed(postSvrInfo, postStream)
//			if postConn != nil{
//				//新连接或发生了重连，重启一个接收routine
//				go postSvrRecvRoutine(postConn, postStream)
//			}
//			frontReq := reqCmd.ReqMsg.(*pb.CPostReq)
//			//log.Printf("pullStream=[%+v]", pullStream)
//			//log.Printf("reqCmd=[%+v]", reqCmd)
//			//log.Printf("frontReq-[%+v]", frontReq)
//			err := postStream.Send(&pb.PostReq{FrontReqId: reqCmd.ReqId, Content:frontReq.Msg})
//			if err != nil {
//				//标记重连，本次业务丢弃
//				postSvrInfo.setNeedRefresh()
//			}
//
//			break
//		case common.CmdFollow:
//			break
//		case common.CmdUnFollow:
//			break
//		case common.CmdLike:
//			break
//		default:
//			log.Panicf("bad cmd[%d], impossible", reqCmd.Cmd)
//		}
//	}
//}


//gRpc proto///////////////////////////////////////////
type serverT struct{}

func (s *serverT) Pull(ctx context.Context, in *pb.CPullReq) (*pb.CPullRsp, error) {
	//统计计数
	atomic.AddInt64(&gReqPullCounter, 1)

	//接受rsp管道时不能死等，需要有超时
	//reqId为key，保存rspChn。超时后销毁。

	//为本次请求获取一个reqId
	reqId := gReqIdGen.getOneId()

	reqCmd := &common.ReqCmdT{ReqId: reqId, Cmd: common.CmdPull, ReqMsg: in}

	//创建rsp管道
	rspChn := make(chan *common.RspCmdT, 1)
	gRspChanMap.insertChn(reqId, rspChn)
	//rsp后就扔掉
	defer gRspChanMap.delChn(reqId)


	//确定一个输送管道然后发送req
	reqChnIndex := reqId % int64(len(gReqChanArr))
	gReqChanArr[reqChnIndex] <- reqCmd

	//等待rsp或超时
	select {
	case <-ctx.Done():
		return &pb.CPullRsp{MsgArr:[]*pb.MsgData{}}, errors.New("timeout")
	case rsp, ok := <-rspChn:
		if !ok{
			rlog.Printf("<-rspChn not ok in pull. reqId=%d", reqId)
			return &pb.CPullRsp{MsgArr:[]*pb.MsgData{}}, errors.New("error")
		}
		msgArr := rsp.RspMsg.([]*pb.MsgData)

		//统计计数
		atomic.AddInt64(&gRspPullCounter, 1)
		return &pb.CPullRsp{MsgArr:msgArr}, nil
	}
}
func (s *serverT) Post(ctx context.Context, in *pb.CPostReq) (*pb.CPostRsp, error) {
	//统计计数
	atomic.AddInt64(&gReqPostCounter, 1)

	//为本次请求获取一个reqId
	reqId := gReqIdGen.getOneId()

	reqCmd := &common.ReqCmdT{ReqId: reqId, Cmd: common.CmdPost, ReqMsg: in}

	//创建rsp管道用来接收本次请求的rsp
	rspChn := make(chan *common.RspCmdT, 1)
	gRspChanMap.insertChn(reqId, rspChn)
	defer gRspChanMap.delChn(reqId)

	//确定一个输送管道然后发送req
	reqChnIndex := reqId % int64(len(gReqChanArr))
	gReqChanArr[reqChnIndex] <- reqCmd

	//等待rsp或超时
	select {
	case <-ctx.Done():
		return &pb.CPostRsp{}, errors.New("timeout")
	case rsp, ok := <-rspChn:
		if !ok{
			rlog.Printf("<-rspChn not ok in pull. reqId=%d", reqId)
			return &pb.CPostRsp{}, errors.New("error")
		}
		postRsp := rsp.RspMsg.(*pb.PostRsp)

		//统计计数
		atomic.AddInt64(&gRspPostCounter, 1)
		return &pb.CPostRsp{MsgId:postRsp.MsgId}, nil
	}
}
func (s *serverT) Follow(ctx context.Context, in *pb.CFollowReq) (*pb.CFollowRsp, error) {
	//统计计数
	atomic.AddInt64(&gReqFollowCounter, 1)

	err := callRelationChgSvr(ctx, true, in.UserId, in.FollowedUserId, &gRelationChgSvrConn)
	if err != nil{
		rlog.Printf("Follow callRelationChgSvr failed. userid=%d followedId=%d err=[%+v]", in.UserId, in.FollowedUserId, err)
		return &pb.CFollowRsp{}, err
	}

	//统计计数
	atomic.AddInt64(&gRspFollowCounter, 1)
	return &pb.CFollowRsp{}, nil
}
func (s *serverT) UnFollow(ctx context.Context, in *pb.CUnFollowReq) (*pb.CUnFollowRsp, error) {
	//统计计数
	atomic.AddInt64(&gReqUnFollowCounter, 1)

	err := callRelationChgSvr(ctx, false, in.UserId, in.UnFollowedUserId, &gRelationChgSvrConn)
	if err != nil{
		rlog.Printf("Follow callRelationChgSvr failed. userid=%d followedId=%d err=[%+v]", in.UserId, in.UnFollowedUserId, err)
		return &pb.CUnFollowRsp{}, err
	}

	//统计计数
	atomic.AddInt64(&gRspUnFollowCounter, 1)
	return &pb.CUnFollowRsp{}, nil
}
func (s *serverT) Like(ctx context.Context, in *pb.CLikeReq) (*pb.CLikeRsp, error) {
	return &pb.CLikeRsp{}, nil
}
func (s *serverT) CheckAvail(ctx context.Context, in *pb.FrontCheckAvailReq) (*pb.FrontCheckAvailRsp, error) {
	//log.Printf("CheckAvail return nil")
	return &pb.FrontCheckAvailRsp{}, nil
}
//gRpc proto end///////////////////////////////////////////

var gRelationChgSvrConn = grpcConn{Conn:nil}
func callRelationChgSvr(ctx context.Context, followNotUnFollow bool, userId int64, followedId int64, svrConn *grpcConn) error{
	//允许重试一次
	for i:=0; i<2; i++{
		if svrConn.Conn == nil{
			//还未链接则链接
			conn, err := grpc.Dial(gConfig.RelationChgSvrAddr, grpc.WithInsecure())
			if err != nil {
				rlog.Printf("connect RelationChgSvr failed. [%+v]", err)
				//连不上，视为服务down掉，忽略
				return errors.New("connect RelationChgSvr failed")
			}
			client := pb.NewRelationChangeSvrClient(conn)
			svrConn.Conn = conn
			svrConn.Client = client
		}

		//
		err := errors.New("")
		if followNotUnFollow{
			_, err = svrConn.Client.(pb.RelationChangeSvrClient).Follow(ctx, &pb.FollowReq{UserId:userId, FollowedUserId:followedId})
		}else{
			_, err = svrConn.Client.(pb.RelationChangeSvrClient).UnFollow(ctx, &pb.UnFollowReq{UserId:userId, UnFollowedUserId:followedId})
		}

		if err != nil {
			rlog.Printf("callRelationChgSvr failed. [%+v]", err)

			_ = svrConn.Conn.Close()
			svrConn.Conn = nil
			//重试一次
			continue
		}
		return nil
	}

	return errors.New("change relation failed")
}

func startConfigRefresh(confPath string){

	for {
		//
		confBytes, err := ioutil.ReadFile(confPath)
		if err != nil {
			rlog.Printf("Read config file failed.[%s][%+v]", confPath, err)
		}else{
			//解析
			config := shareSvrArrConfigT{}
			err = json.Unmarshal(confBytes, &config)
			if err != nil {
				rlog.Printf("Read config file failed.[%s][%+v]", confPath, err)
			}else{
				//更新配置
				{
					gPostSvrData.Lock()
					gPostSvrData.SvrAddrArr = config.PostSvrAddrArr
					gPostSvrData.curIndex = 0
					gPostSvrData.Unlock()

					gPullSvrData.Lock()
					gPullSvrData.SvrAddrArr = config.PullSvrAddrArr
					gPullSvrData.curIndex = 0
					gPullSvrData.Unlock()
				}
			}
		}

		time.Sleep(10 * time.Second)
	}
}