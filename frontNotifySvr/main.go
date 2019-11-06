/*
@Author : Ryan.wuxiaoyong
*/

package main

import (
	"WeiBo/common/grpcStatsHandler"
	"context"
	"encoding/json"
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"io"
	"sync/atomic"

	//"io"

	//"go/types"
	"io/ioutil"
	//"log"
	"net"
	"sync"
	"time"
	"os"

	"WeiBo/common"
	pb "WeiBo/common/protobuf"
	"WeiBo/common/rlog"
)

//配置文件格式
type configType struct {
	MyName string `json: "myName"`
	RlogSvrAddr string `json: "rlogSvrAddr"`
	ListenAddr string  `json: "listenAddr"`
	FrontNotifySvrId int32 `json: "frontNotifySvrId"`
	FollowedSvrAddr string `json: "followedSvrAddr"`
}
//配置文件数据对象
var gConfig = &configType{}

type shareSvrArrConfigT struct {
	PushSvrAddrArr []string `json: "pushSvrAddrArr"`
}

//pushSvr负荷分担数据/////////////////////////////////////////
type pushSvrShareT struct {
	sync.RWMutex

	SvrAddrArr []string //平行负荷分担，轮询
	curIndex int //当前轮询index
}
//
var gPushSvrData = pushSvrShareT{}

//每一个userId的online数据
type userIdOnlineDataT struct{
	//接收notify信息的管道
	notifyChn chan *pb.CNotify

	//心跳超时定时器
	idleTimer *time.Timer

	//offline同步管道
	offlineSyncChn chan int
}
type userIdOnlineDataMapT struct {
	sync.Mutex

	dataMap map[int64]userIdOnlineDataT
}
var gUserIdOnlineDataMap = userIdOnlineDataMapT{sync.Mutex{}, map[int64]userIdOnlineDataT{}}


//接受req的管道结构，所有连接的请求都汇聚到这里，并由管道后的roution进行处理
var gReqChan = make(chan *common.ReqCmdT, 10000)

//客户端连接统计
var gConnCounterHandle = grpcStatsHandler.ConnCounterHandler{}

//发送通知计数
var gSendNotifyCount int64

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

	configRefreshSyncChn := make(chan int, 1)
	//启动配置文件刷新routine
	go startConfigRefresh(confPath, configRefreshSyncChn)

	go notifyRecvRoutine(configRefreshSyncChn)

	for i:=0; i<4; i++ {
		go onlineProcRoutine()
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
	//设置连接断开回调
	gConnCounterHandle.ConnEndCallBack = connEndCallBack
	handler := grpc.StatsHandler(&gConnCounterHandle)
	grpcServer := grpc.NewServer(svrOpt, handler)
	pb.RegisterFrontNotifySvrServer(grpcServer, &serverT{})
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

			gUserIdOnlineDataMap.Lock()
			onlineCount := len(gUserIdOnlineDataMap.dataMap)
			gUserIdOnlineDataMap.Unlock()
			data = rlog.StatPointData{Name:"onlineUserCount", Data:int64(onlineCount)}
			dataArr = append(dataArr, data)

			data = rlog.StatPointData{Name:"sendNotify", Data:gSendNotifyCount}
			dataArr = append(dataArr, data)

			data = rlog.StatPointData{Name:"onlineReqChanLen", Data:int64(len(gReqChan))}
			dataArr = append(dataArr, data)

		}
		rlog.StatPoint("totalReport", dataArr)
	}
}

//connId到userId的map
type connId2userIdT struct {
	sync.Mutex

	connId2userIdMap map[int64]int64
}
var gConnId2userId = connId2userIdT{sync.Mutex{}, make(map[int64]int64)}
//连接断开时回调
func connEndCallBack(connId int64){
	//根据connId找到userId
	gConnId2userId.Lock()
	userId, ok := gConnId2userId.connId2userIdMap[connId]
	delete(gConnId2userId.connId2userIdMap, connId)
	gConnId2userId.Unlock()
	if !ok{
		//rlog.Printf("gConnId2userId.connId2userIdMap[%d] is not ok", connId)
		return
	}
	//rlog.Printf("gConnId2userId.connId2userIdMap[%d]=userId=%d", connId, userId)

	gUserIdOnlineDataMap.Lock()
	offlineChn := gUserIdOnlineDataMap.dataMap[userId].offlineSyncChn
	gUserIdOnlineDataMap.Unlock()

	if len(offlineChn) == 0{
		offlineChn<- 1
	}
}

func onlineProcRoutine(){

	//服务器地址到链接的map
	addr2ConnMap := make(map[string]grpcConn)

	for{
		req, ok := <-gReqChan
		if !ok{
			rlog.Fatalf("<-gReqChan closed. shit")
			return
		}

		//
		sendReqToPushSvr(addr2ConnMap, req)
		sendReqToFollowedSvr(addr2ConnMap, req)
	}
}
func sendReqToPushSvr(addr2ConnMap map[string]grpcConn, req *common.ReqCmdT){
	//先把svr地址拷贝出来
	gPushSvrData.Lock()
	//用值拷贝，防止引用用一个数组，gUserMsgIdSvrData中的数组是处于竞态环境
	svrAddrArr := make([]string, len(gPushSvrData.SvrAddrArr))
	copy(svrAddrArr, gPushSvrData.SvrAddrArr)
	gPushSvrData.Unlock()

	for _, addr := range svrAddrArr{
		//允许重试一次
		for i:=0;i<2;i++{
			connData, ok := addr2ConnMap[addr]
			if !ok{
				//还未链接则链接
				conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock())
				if err != nil {
					rlog.Printf("connect pushsvr failed. addr=[%s] [%+v]", addr, err)
					break
				}
				client := pb.NewPushSvrClient(conn)
				connData = grpcConn{Conn:conn, Client:client}
				addr2ConnMap[addr] = connData
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			switch req.Cmd {
			case common.CmdOnline:
				_, err := connData.Client.(pb.PushSvrClient).Online(ctx, &pb.OnlineReq{UserId:req.ReqMsg.(common.ReqOnline).UserId, FrontNotifySvrId:req.ReqMsg.(common.ReqOnline).FrontNotifySvrId})
				cancel()
				if err != nil{
					//链接失效
					_ = connData.Conn.Close()
					delete(addr2ConnMap, addr)
					//重连一次
					continue
				}
				break
			case common.CmdOffline:
				_, err := connData.Client.(pb.PushSvrClient).Offline(ctx, &pb.OfflineReq{UserId:req.ReqMsg.(common.ReqOffline).UserId})
				cancel()
				//rlog.Printf("call offline to pushSvr. userId=%d", req.ReqMsg.(common.ReqOffline).UserId)
				if err != nil{
					rlog.Printf("call offline to pushSvr failed.err=[%+v]", err)
					//链接失效
					_ = connData.Conn.Close()
					delete(addr2ConnMap, addr)
					//重连一次
					continue
				}
				break
			default:
				rlog.Printf("bad cmd=%d", req.Cmd)
				break
			}
			break
		}
	}
}
func sendReqToFollowedSvr(addr2ConnMap map[string]grpcConn, req *common.ReqCmdT){
	addr := gConfig.FollowedSvrAddr

	{
		//允许重试一次
		for i:=0;i<2;i++{
			connData, ok := addr2ConnMap[addr]
			if !ok{
				//还未链接则链接
				conn, err := grpc.Dial(addr, grpc.WithInsecure())
				if err != nil {
					rlog.Printf("connect pushsvr failed. addr=[%s] [%+v]", addr, err)
					break
				}
				client := pb.NewFollowedSvrClient(conn)
				connData = grpcConn{Conn:conn, Client:client}
				addr2ConnMap[addr] = connData
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			switch req.Cmd {
			case common.CmdOnline:
				_, err := connData.Client.(pb.FollowedSvrClient).Online(ctx, &pb.OnlineReq{UserId:req.ReqMsg.(common.ReqOnline).UserId, FrontNotifySvrId:req.ReqMsg.(common.ReqOnline).FrontNotifySvrId})
				cancel()
				if err != nil{
					//链接失效
					_ = connData.Conn.Close()
					delete(addr2ConnMap, addr)
					//重连一次
					continue
				}
				break
			case common.CmdOffline:
				_, err := connData.Client.(pb.FollowedSvrClient).Offline(ctx, &pb.OfflineReq{UserId:req.ReqMsg.(common.ReqOffline).UserId})
				cancel()
				//rlog.Printf("call offline to followedSvr. userId=%d", req.ReqMsg.(common.ReqOffline).UserId)
				if err != nil{
					rlog.Printf("call offline to followedSvr failed.err=[%+v]", err)
					//链接失效
					delete(addr2ConnMap, addr)
					//重连一次
					continue
				}
				break
			default:
				rlog.Printf("bad cmd=%d", req.Cmd)
				break
			}
			break
		}
	}
}

//获取地址对应的链接，如果还未链接则链接
type grpcConn struct {
	Conn *grpc.ClientConn
	Client interface{}
}
type addrConnMapT struct{
	sync.Mutex

	dataMap map[string]grpcConn
}

func notifyRecvRoutine(configRefreshSyncChn chan int){

	//服务器地址到链接的map
	addr2ConnMap := addrConnMapT{sync.Mutex{}, map[string]grpcConn{}}

	for{
		<-configRefreshSyncChn

		//先把svr地址拷贝出来
		gPushSvrData.Lock()
		//用值拷贝，防止引用用一个数组，gUserMsgIdSvrData中的数组是处于竞态环境
		svrAddrArr := make([]string, len(gPushSvrData.SvrAddrArr))
		copy(svrAddrArr, gPushSvrData.SvrAddrArr)
		gPushSvrData.Unlock()

		for _, addr := range svrAddrArr{
			addr2ConnMap.Lock()
			_, ok := addr2ConnMap.dataMap[addr]
			addr2ConnMap.Unlock()

			if !ok{
				//还未链接则链接
				conn, err := grpc.Dial(addr, grpc.WithInsecure())
				if err != nil {
					rlog.Printf("connect pushsvr failed. addr=[%s] [%+v]", addr, err)
					continue
				}
				client := pb.NewPushSvrClient(conn)
				connData := grpcConn{Conn:conn, Client:client}
				addr2ConnMap.Lock()
				addr2ConnMap.dataMap[addr] = connData
				addr2ConnMap.Unlock()

				go recvNotify(addr, &addr2ConnMap)
			}
		}
	}
}
func recvNotify(addr string, addr2ConnMap *addrConnMapT){
	addr2ConnMap.Lock()
	connData, ok :=addr2ConnMap.dataMap[addr]
	addr2ConnMap.Unlock()

	if !ok {
		rlog.Printf("shit recvNotify addr not exist")
		return
	}

	//ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	stream, err := connData.Client.(pb.PushSvrClient).CreateNotifyStream(context.Background(), &pb.CreateNotifyStreamReq{FrontNotifySvrId:gConfig.FrontNotifySvrId})
	if err != nil {
		rlog.Printf("CreateNotifyStream failed. [%+v]", err)

		//删掉
		_ = connData.Conn.Close()
		addr2ConnMap.Lock()
		delete(addr2ConnMap.dataMap, addr)
		addr2ConnMap.Unlock()
		return
	}

	for {
		notify, err := stream.Recv()
		if err == io.EOF{
			rlog.Printf("NotifyStream recv failed. [%+v]", err)

			//删掉
			_ = connData.Conn.Close()
			addr2ConnMap.Lock()
			delete(addr2ConnMap.dataMap, addr)
			addr2ConnMap.Unlock()
			return
		}
		if err != nil{
			rlog.Printf("NotifyStream recv failed. [%+v]", err)

			//删掉
			_ = connData.Conn.Close()
			addr2ConnMap.Lock()
			delete(addr2ConnMap.dataMap, addr)
			addr2ConnMap.Unlock()
			return
		}
		//rlog.Printf("notify-[%+v]", notify)

		for _, notifyUserId := range notify.NotifyUserId{
			gUserIdOnlineDataMap.Lock()
			userIdOnlineData, ok := gUserIdOnlineDataMap.dataMap[notifyUserId]
			chn := userIdOnlineData.notifyChn
			gUserIdOnlineDataMap.Unlock()

			if !ok{
				//用户下线了
				continue
			}

			atomic.AddInt64(&gSendNotifyCount, 1)
			chn<- &pb.CNotify{NotifyUserId:notifyUserId, PostUserId:notify.PostUserId, MsgId:notify.MsgId}
		}
	}
}

//gRpc proto///////////////////////////////////////////
type serverT struct{}

func (s *serverT)CreateNotifyStream(in *pb.CCreateNotifyStreamReq, stream pb.FrontNotifySvr_CreateNotifyStreamServer)error{
	connId, ok := gConnCounterHandle.GetConnId(stream.Context())
	if ok{
		//简历connId到userId的映射
		gConnId2userId.Lock()
		gConnId2userId.connId2userIdMap[connId] = in.UserId
		gConnId2userId.Unlock()
	}

	userId := in.UserId

	//保存当前userid在线数据
	notifyChn := make(chan *pb.CNotify, 50)
	idleTimer := time.NewTimer(common.ClientMaxIdleSec*time.Second)
	offlineSyncChn := make(chan int, 5)
	gUserIdOnlineDataMap.Lock()
	gUserIdOnlineDataMap.dataMap[userId] = userIdOnlineDataT{notifyChn:notifyChn, idleTimer:idleTimer, offlineSyncChn:offlineSyncChn}
	gUserIdOnlineDataMap.Unlock()

	//发送online
	gReqChan<- &common.ReqCmdT{ReqId:1, Cmd:common.CmdOnline, ReqMsg:common.ReqOnline{UserId:userId, FrontNotifySvrId:gConfig.FrontNotifySvrId}}

	for{
		select {
		case <-idleTimer.C:
			//rlog.Printf("idleTimer timeout. userId=%d", userId)
			gUserIdOnlineDataMap.Lock()
			if notifyChn == gUserIdOnlineDataMap.dataMap[userId].notifyChn{
				//是我自己，删掉
				delete(gUserIdOnlineDataMap.dataMap, userId)
				gUserIdOnlineDataMap.Unlock()

				//发送offline
				gReqChan<- &common.ReqCmdT{ReqId:1, Cmd:common.CmdOffline, ReqMsg:common.ReqOffline{UserId:userId}}
				rlog.Printf("idleTimer timeout. send CmdOffline, userId=%d", userId)
			}else{
				gUserIdOnlineDataMap.Unlock()
			}

			return errors.New("idle timeout")
		case <-offlineSyncChn:
			gUserIdOnlineDataMap.Lock()
			if notifyChn == gUserIdOnlineDataMap.dataMap[userId].notifyChn{
				//是我自己，删掉
				delete(gUserIdOnlineDataMap.dataMap, userId)
				gUserIdOnlineDataMap.Unlock()


				//发送offline
				gReqChan<- &common.ReqCmdT{ReqId:1, Cmd:common.CmdOffline, ReqMsg:common.ReqOffline{UserId:userId}}
				return nil
			}else{
				gUserIdOnlineDataMap.Unlock()
			}
		case notify, ok:= <-notifyChn:
			if !ok {
				rlog.Printf("notifyChn closed :( ")
				return nil
			}
			err := stream.Send(notify)
			if err != nil{
				gUserIdOnlineDataMap.Lock()
				if notifyChn == gUserIdOnlineDataMap.dataMap[userId].notifyChn{
					//是我自己，删掉
					delete(gUserIdOnlineDataMap.dataMap, userId)
					gUserIdOnlineDataMap.Unlock()

					//发送offline
					gReqChan<- &common.ReqCmdT{ReqId:1, Cmd:common.CmdOffline, ReqMsg:common.ReqOffline{UserId:userId}}

					return errors.New("send err")
				}else{
					gUserIdOnlineDataMap.Unlock()
				}
				return errors.New("send err")
			}

			break
		}
	}
}

func (s *serverT) Offline(ctx context.Context, in *pb.COfflineReq) (*pb.COfflineRsp, error) {
	gUserIdOnlineDataMap.Lock()
	offlineChn := gUserIdOnlineDataMap.dataMap[in.UserId].offlineSyncChn
	gUserIdOnlineDataMap.Unlock()

	offlineChn<- 1

	return &pb.COfflineRsp{}, nil
}

func (s *serverT) HeartBeat(ctx context.Context, in *pb.CHeartBeatReq) (*pb.CHeartBeatRsp, error) {
	//log.Printf("hearBeat userId=%d", in.UserId)
	gUserIdOnlineDataMap.Lock()

	timer := gUserIdOnlineDataMap.dataMap[in.UserId].idleTimer
	if timer != nil{
		timer.Reset(common.ClientMaxIdleSec*time.Second)
	}

	gUserIdOnlineDataMap.Unlock()

	//上报online
	gReqChan<- &common.ReqCmdT{ReqId:1, Cmd:common.CmdOnline, ReqMsg:common.ReqOnline{UserId:in.UserId, FrontNotifySvrId:gConfig.FrontNotifySvrId}}

	return &pb.CHeartBeatRsp{}, nil
}

func (s *serverT) CheckAvail(ctx context.Context, in *pb.FrontNotifyCheckAvailReq) (*pb.FrontNotifyCheckAvailRsp, error) {
	//log.Printf("CheckAvail return nil")
	return &pb.FrontNotifyCheckAvailRsp{}, nil
}
//gRpc proto end///////////////////////////////////////////


func startConfigRefresh(confPath string, configRefreshSyncChn chan int){

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
					gPushSvrData.Lock()

					gPushSvrData.SvrAddrArr = config.PushSvrAddrArr
					gPushSvrData.curIndex = 0

					gPushSvrData.Unlock()
				}

				//同步通知
				configRefreshSyncChn<- 1
			}
		}

		time.Sleep(1 * time.Second)
	}
}