/*
@Author : Ryan.wuxiaoyong
*/

package main

import (
	"WeiBo/common/rlog"
	"context"
	"encoding/json"
	"google.golang.org/grpc/keepalive"
	"time"

	"errors"
	"google.golang.org/grpc"
	//"io"

	"io/ioutil"
	//"log"
	"net"
	"sync"
	"os"

	pb "WeiBo/common/protobuf"
)

//配置文件格式
type configType struct {
	MyName string `json: "myName"`
	RlogSvrAddr string `json: "rlogSvrAddr"`
	ListenAddr string  `json: "listenAddr"`
	FollowedSvrAddr string  `json: "followedSvrAddr"`
}
//配置文件数据对象
var gConfig = &configType{}

//push请求内部输送管道，所有push请求都汇聚到这个管道
var gPushReqChn = make(chan *pb.PushReq, 10000)

//frontNotifySvrId到notifyChan的map
type frontNotifySvrId2NotifyChanMapT struct {
	sync.RWMutex

	svrId2ChanMap map[int32]chan *pb.Notify
}
var gFrontNotifySvrId2NotifyChanMap = frontNotifySvrId2NotifyChanMapT{sync.RWMutex{}, make(map[int32]chan *pb.Notify)}
func (m *frontNotifySvrId2NotifyChanMapT)store(svrId int32, chn chan*pb.Notify){
	m.Lock()
	defer m.Unlock()
	m.svrId2ChanMap[svrId] = chn
}

//onlineUser到FrontNotifySvr的数据结构
type userId2FrontNotifySvrIdT struct{
	sync.RWMutex

	dataMap map[int64]int32
}
var gUserId2FrontNotifySvrMap = userId2FrontNotifySvrIdT{sync.RWMutex{}, map[int64]int32{}}
func (m *userId2FrontNotifySvrIdT)getSvrId(userId int64) (int32, bool){
	m.RLock()
	defer m.RUnlock()

	svrId, ok := m.dataMap[userId]
	return svrId, ok
}
func (m *userId2FrontNotifySvrIdT)store(userId int64, svrId int32){
	m.Lock()
	defer m.Unlock()

	m.dataMap[userId] = svrId
}
func (m *userId2FrontNotifySvrIdT)delete(userId int64){
	m.Lock()
	defer m.Unlock()

	delete(m.dataMap, userId)
}

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

	//启动push routine群
	for i:=0; i<1000; i++{
		go pushRoutine()
	}

	//开启数据打点routine
	go statReportRoutine()

	//开启grpc服务
	lis, err := net.Listen("tcp", gConfig.ListenAddr)
	if err != nil {
		rlog.Fatalf("failed to listen: %+v", err)
	}
	rlog.Printf("begin to Listen [%s]", gConfig.ListenAddr)
	svrOpt1 := grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime:5*time.Minute, PermitWithoutStream:true})
	svrOpt2 := grpc.KeepaliveParams(keepalive.ServerParameters{Time:1*time.Minute})
	grpcServer := grpc.NewServer(svrOpt1, svrOpt2)
	pb.RegisterPushSvrServer(grpcServer, &serverT{})
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
			data := rlog.StatPointData{Name:"reqChanLen", Data:int64(len(gPushReqChn))}
			dataArr = append(dataArr, data)

			gUserId2FrontNotifySvrMap.RLock()
			onlineCount := len(gUserId2FrontNotifySvrMap.dataMap)
			gUserId2FrontNotifySvrMap.RUnlock()
			data = rlog.StatPointData{Name:"onlineUserCount", Data:int64(onlineCount)}
			dataArr = append(dataArr, data)
		}
		rlog.StatPoint("totalReport", dataArr)
	}
}

//gRpc proto///////////////////////////////////////////
type serverT struct{}

//func (s *serverT) CreatePushStream(stream pb.PushSvr_CreatePushStreamServer)error {
//	for {
//		req, err := stream.Recv()
//		if err == io.EOF{
//			//对方不再发req了
//			log.Printf("pushSvr recv push req EOF")
//			break
//		}
//		if err != nil{
//			log.Printf("pushSvr recv push req err.[%+v]", err)
//			break
//		}
//		//
//		gPushReqChn<- req
//	}
//
//	return errors.New("recv error")
//}

func (s *serverT)Push(ctx context.Context, in *pb.PushReq) (*pb.PushRsp, error){
	gPushReqChn<- in
	return &pb.PushRsp{}, nil
}
func (s *serverT) CreateNotifyStream(in *pb.CreateNotifyStreamReq, stream pb.PushSvr_CreateNotifyStreamServer) error {
	//rlog.Printf("CreateNotifyStream")

	//创建本routine对应的notify接收管道
	notifyChan := make(chan *pb.Notify, 1000)
	gFrontNotifySvrId2NotifyChanMap.store(in.FrontNotifySvrId, notifyChan)
	//rlog.Printf("CreateNotifyStream frontNotifySvrId=%d", in.FrontNotifySvrId)

	//stream保活定时器
	keepaliveTimer := time.NewTimer(1*time.Minute)

	for{
		keepaliveTimer.Reset(1*time.Minute)

		var notify *pb.Notify
		select {
		case <-keepaliveTimer.C:
			//空notify作为心跳保活
			notify = &pb.Notify{}
			break
		case notify = <-notifyChan:
			break
		}

		err := stream.Send(notify)
		if err == nil {
			continue
		}
		rlog.Printf("notifyStream send failed. frontNotifySvr conn down. err=[%+v]", err)

		//原frontNotifySvr down了
		gFrontNotifySvrId2NotifyChanMap.Lock()

		if notifyChan == gFrontNotifySvrId2NotifyChanMap.svrId2ChanMap[in.FrontNotifySvrId]{
			//新svr还未连上来，删掉
			delete(gFrontNotifySvrId2NotifyChanMap.svrId2ChanMap, in.FrontNotifySvrId)

			//
			gFrontNotifySvrId2NotifyChanMap.Unlock()
			//退出
			rlog.Printf("notifyStream send failed. new conn not exist")
			return errors.New("error")
		}
		newChn := gFrontNotifySvrId2NotifyChanMap.svrId2ChanMap[in.FrontNotifySvrId]
		//
		gFrontNotifySvrId2NotifyChanMap.Unlock()

		//新svr练上来了，转发
		newChn<- notify
		rlog.Printf("notifyStream send failed. new conn exist. transfer to new conn")
		//退出
		return errors.New("error")
	}
}

func (s *serverT) Online(ctx context.Context, in *pb.OnlineReq) (*pb.OnlineRsp, error) {
	gUserId2FrontNotifySvrMap.store(in.UserId, in.FrontNotifySvrId)
	return &pb.OnlineRsp{}, nil
}

func (s *serverT) Offline(ctx context.Context, in *pb.OfflineReq) (*pb.OfflineRsp, error) {
	gUserId2FrontNotifySvrMap.delete(in.UserId)
	return &pb.OfflineRsp{}, nil
}
//gRpc proto end///////////////////////////////////////////

func pushRoutine(){
	//服务器地址到链接的map，给userIdMsgSvr和contentSvr的链接用
	addr2ConnMap := make(map[string]grpcConn)

	///////////
	for {
		req := <-gPushReqChn

		//查询followed列表
		followedIdArr, err := getFollowedUserIdArr(req.UserId, addr2ConnMap)
		if err != nil{
			rlog.Printf(" QueryOnlineFollowedList failed. [%+v]", err)
			//丢弃
			break
		}
		//rlog.Printf("followedIdArr=%v", followedIdArr)

		//在归宿frontNotifySvr下聚集
		svrDataMap := make(map[int32][]int64)
		for _, followedId := range *followedIdArr{
			//rlog.Printf("svrMap=%v", gUserId2FrontNotifySvrMap.dataMap)
			svrId, ok := gUserId2FrontNotifySvrMap.getSvrId(followedId)
			if ok{
				svrDataMap[svrId] = append(svrDataMap[svrId], followedId)
			}
		}
		//rlog.Printf("svrDataMap=%v", svrDataMap)

		//向frontNotifySvr对应的管道发消息
		for svrId, userIdArr := range svrDataMap{
			gFrontNotifySvrId2NotifyChanMap.Lock()
			chn, ok := gFrontNotifySvrId2NotifyChanMap.svrId2ChanMap[svrId]
			gFrontNotifySvrId2NotifyChanMap.Unlock()
			if !ok{
				//svr可能是down了，新的还未起来
				rlog.Printf("pushRoutine can not found notifyChan for notifySvrId=%d", svrId)
				continue
			}
			chn<- &pb.Notify{PostUserId:req.UserId, MsgId:req.MsgId, NotifyUserId:userIdArr}
		}
	}
}

//获取地址对应的链接，如果还未链接则链接
type grpcConn struct {
	Conn *grpc.ClientConn
	Client interface{}
}
func getFollowedUserIdArr(userId int64, addr2ConnMap map[string]grpcConn) (*[]int64, error){
	//允许重试一次
	for i:=0; i<2; i++{
		//获取地址对应的链接
		var connData grpcConn
		{
			var ok bool
			connData, ok = addr2ConnMap[gConfig.FollowedSvrAddr]
			if !ok{
				//还未链接则链接
				conn, err := grpc.Dial(gConfig.FollowedSvrAddr, grpc.WithInsecure())
				if err != nil {
					rlog.Printf("connect FollowSvr failed. [%+v]", err)
					//连不上，视为服务down掉，忽略
					return &[]int64{}, errors.New("connect FollowSvr failed")
				}
				client := pb.NewFollowedSvrClient(conn)
				connData = grpcConn{Conn:conn, Client:client}
				addr2ConnMap[gConfig.FollowedSvrAddr] = connData
			}
		}

		//
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		rsp, err := connData.Client.(pb.FollowedSvrClient).QueryFollowedList(ctx, &pb.QueryFollowedListReq{UserId:userId})
		cancel()
		if err != nil {
			rlog.Printf("QueryFollowedList failed. [%+v]", err)
			_ = connData.Conn.Close()
			delete(addr2ConnMap, gConfig.FollowedSvrAddr)
			//重试一次
			continue
		}
		return &rsp.FollowedIdArr, nil
	}

	return &[]int64{}, errors.New("QueryFollowedList failed")
}
