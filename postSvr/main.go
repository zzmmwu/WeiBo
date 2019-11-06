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
	"io/ioutil"
	//"log"
	"net"
	"os"
	"sync"
	"time"

	pb "WeiBo/common/protobuf"
	"WeiBo/common/rlog"
)

//配置文件格式
type configType struct {
	MyName string `json: "myName"`
	RlogSvrAddr string `json: "rlogSvrAddr"`
	ListenAddr string  `json: "listenAddr"`
	MsgIdGenSvrAddr string `json: "msgIdGenSvrAddr"`
	DBSvrAddr string `json: "dbSvrAddr"`
}
//配置文件数据对象
var gConfig = &configType{}

type shareSvrArrConfigT struct {
	UserMsgIdSvrArr [][]string `json: "userMsgIdSvrArr"`
	ContentSvrArr [][]string `json: "contentSvrArr"`
	PushSvrAddrArr []string `json: "pushSvrAddrArr"`
}

//userMsgIdSvr分片数据/////////////////////////////////////////
type userMsgIdSvrDataT struct {
	sync.RWMutex

	UserMsgIdSvrArr []userMsgIdSvrShareArr //分片服务地址列表
}
type userMsgIdSvrShareArr struct{
	SvrAddrArr []string //平行负荷分担服务器
}
//userMsgIdSvr分片数据对象
var gUserMsgIdSvrData = userMsgIdSvrDataT{}


//contentSvr分片数据/////////////////////////////////////////
type contentSvrDataT struct {
	sync.RWMutex

	contentSvrArr []contentShareArr //分片服务地址列表
}
type contentShareArr struct{
	SvrAddrArr []string //平行负荷分担服务器
}
//contentSvr分片数据对象
var gContentSvrData = contentSvrDataT{}

//pushSvr负荷分担数据/////////////////////////////////////////
type pushSvrShareT struct {
	sync.RWMutex

	SvrAddrArr []string //平行负荷分担，轮询
	curIndex int //当前轮询index
}
//
var gPushSvrData = pushSvrShareT{}

//post请求内部输送管道，所有请求都汇聚到这个管道
type postReqPack struct {
	RspChn chan *pb.PostRsp
	Req *pb.PostReq
}
var gPostReqChn = make(chan *postReqPack, 10000)

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

	//启动post routine群
	for i:=0; i<100; i++{
		go postRoutine()
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
	pb.RegisterPostSvrServer(grpcServer, &serverT{})
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
			data := rlog.StatPointData{Name:"reqChanLen", Data:int64(len(gPostReqChn))}
			dataArr = append(dataArr, data)
		}
		rlog.StatPoint("totalReport", dataArr)
	}
}

//gRpc proto///////////////////////////////////////////
type serverT struct{}

func (s *serverT) CreateStream(stream pb.PostSvr_CreateStreamServer) error {
	//创建rsp接受管道
	rspChn := make(chan *pb.PostRsp, 1000)
	//独立一个routine处理回复
	reqEofSyncChn := make(chan int, 1)
	noMoreRspSyncChn := make(chan int, 1)
	go postRspRecvRoutine(rspChn, stream, reqEofSyncChn, noMoreRspSyncChn)

	for {
		req, err := stream.Recv()
		if err == io.EOF{
			//对方不再发req了
			rlog.Printf("postSvr recv req EOF")
			break
		}
		if err != nil{
			rlog.Printf("postSvr recv err.[%+v]", err)
			break
		}
		if req.FrontReqId == 0 {
			//如果是心跳包（空包）则直接返回
			rspChn<- &pb.PostRsp{}
		}else{
			//
			reqPack := &postReqPack{RspChn:rspChn, Req:req}
			gPostReqChn<- reqPack
		}

	}

	//
	reqEofSyncChn<- 1
	//等rsp接受routine结束
	<-noMoreRspSyncChn
	return nil
}

func (s *serverT) CheckAvail(ctx context.Context, in *pb.CheckAvailReq) (*pb.CheckAvailRsp, error) {
	//log.Printf("CheckAvail return nil")
	return &pb.CheckAvailRsp{}, nil
}
//gRpc proto end///////////////////////////////////////////

//pullSvr上如果30秒没有rsp了，且reqStream已经EOF则通知req routine
func postRspRecvRoutine(chn chan *pb.PostRsp, stream pb.PostSvr_CreateStreamServer, reqEofSyncChn chan int, noMoreRspSyncChn chan int){
	//定时器时长，初始为100年
	noRspTimerDur := 999999*time.Hour
	//无rsp定时器
	noRspTimer := time.NewTimer(noRspTimerDur)

	for {
		select {
		case rsp:= <-chn:
			//重置定时器
			noRspTimer.Reset(noRspTimerDur)
			//log.Printf("%+v", rsp)
			err := stream.Send(rsp)
			if err != nil{
				rlog.Printf("pullSvr send err.[%+v]", err)
				//虽然此时stream已断，但chn中可能还会进来更多rsp需要处理，所以不能退出。需要把所有潜在的rsp处理完
				//没有更多req过来了，30秒定时
				noRspTimerDur = 30*time.Second
				noRspTimer.Reset(noRspTimerDur)
			}
			break
		case <-reqEofSyncChn:
			//没有更多req过来了，30秒定时
			rlog.Printf("<-reqEofSyncChn")
			noRspTimerDur = 30*time.Second
			noRspTimer.Reset(noRspTimerDur)
			break
		case <-noRspTimer.C:
			noMoreRspSyncChn<- 1
			return
		}
	}
}

type dbClientT struct {
	conn *grpc.ClientConn
	client pb.DbSvrClient
}

func postRoutine(){

	//服务器地址到链接的map
	addr2ConnMap := make(map[string]grpcConn)

	var dbClient dbClientT

	for {
		reqPack := <-gPostReqChn

		//获取一个msgId
		msgId, err := genMsgId(addr2ConnMap)
		if err != nil{
			rlog.Printf("genMsgId failed. ")
			//丢弃
			continue
		}
		//rlog.Printf("msgId %d", msgId)

		//
		reqPack.Req.Content.MsgId = msgId

		//先存DB
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = storeToDB(ctx, &dbClient, reqPack.Req.Content)
		cancel()
		if err != nil{
			rlog.Printf("storeToDB failed. content=[%+v]", reqPack.Req.Content)
			//丢弃
			continue
		}

		//再保存contentSvr
		//这里无需向再保存contentSvr 发起post操作，contentSvr在pull时发现没有此msg则会从DB加载
		//test
		//err = storeContent(reqPack.Req.Content, addr2ConnMap)
		//if err != nil{
		//	log.Printf("storeContent failed. content=[%+v]", reqPack.Req.Content)
		//	//丢弃
		//	continue
		//}

		//再存userMsgIdSvr
		err = storeUsrMsgId(reqPack.Req.Content.UserId, reqPack.Req.Content.MsgId, addr2ConnMap)
		if err != nil{
			rlog.Printf("storeUsrMsgId failed. content=[%+v]", reqPack.Req.Content)
			//丢弃
			continue
		}

		//再向PushSvr发送通知
		err = sendToPushSvr(reqPack.Req.Content.UserId, reqPack.Req.Content.MsgId, addr2ConnMap)
		if err != nil{
			rlog.Printf("sendToPushSvr failed. content=[%+v]", reqPack.Req.Content)
			//丢弃
			continue
		}

		rsp := pb.PostRsp{FrontReqId: reqPack.Req.FrontReqId, UserId: reqPack.Req.Content.UserId, MsgId: reqPack.Req.Content.MsgId}
		//rlog.Printf("%+v", rsp)

		reqPack.RspChn <- &rsp
	}
}

func genMsgId(addr2ConnMap map[string]grpcConn) (int64, error){
	//允许重试一次
	for i:=0; i<2; i++{
		//获取地址对应的链接
		var connData grpcConn
		{
			var ok bool
			connData, ok = addr2ConnMap[gConfig.MsgIdGenSvrAddr]
			if !ok{
				//还未链接则链接
				conn, err := grpc.Dial(gConfig.MsgIdGenSvrAddr, grpc.WithInsecure())
				if err != nil {
					rlog.Printf("connect MsgIdGenSvrAddr failed. [%+v]", err)
					//连不上，视为服务down掉，忽略
					return 0, errors.New("connect MsgIdGenSvrAddr failed")
				}
				client := pb.NewMsgIdGeneratorClient(conn)
				connData = grpcConn{Conn:conn, Client:client}
				addr2ConnMap[gConfig.MsgIdGenSvrAddr] = connData
			}
		}

		//
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		rsp, err := connData.Client.(pb.MsgIdGeneratorClient).GenMsgId(ctx, &pb.GenMsgIdReq{})
		cancel()
		if err != nil {
			rlog.Printf("GenMsgId failed. [%+v]", err)
			_ = connData.Conn.Close()
			delete(addr2ConnMap, gConfig.MsgIdGenSvrAddr)
			//重试一次
			continue
		}
		return rsp.MsgId, nil
	}

	return 0, errors.New("GenMsgId failed")
}

//获取地址对应的链接，如果还未链接则链接
type grpcConn struct {
	Conn *grpc.ClientConn
	Client interface{}
}
//把msgId挂到user下
func storeUsrMsgId(userId int64, msgId int64, addr2ConnMap map[string]grpcConn) error {
	//根据userId分片，找出所有对应的usrMsgIdSvr
	var svrAddrArr []string
	{
		gUserMsgIdSvrData.RLock()

		index := userId%int64(len(gUserMsgIdSvrData.UserMsgIdSvrArr))
		//用值拷贝，防止引用用一个数组，gUserMsgIdSvrData中的数组是处于竞态环境
		svrAddrArr = make([]string, len(gUserMsgIdSvrData.UserMsgIdSvrArr[index].SvrAddrArr))
		copy(svrAddrArr, gUserMsgIdSvrData.UserMsgIdSvrArr[index].SvrAddrArr)

		gUserMsgIdSvrData.RUnlock()
	}

	//
	for _, addr := range svrAddrArr{
		//允许重试一次
		for i:=0; i<2; i++{
			//获取地址对应的链接
			var connData grpcConn
			{
				var ok bool
				connData, ok = addr2ConnMap[addr]
				if !ok{
					//还未链接则链接
					conn, err := grpc.Dial(addr, grpc.WithInsecure())
					if err != nil {
						rlog.Printf("connect usrMsgIdSvr failed. addr=[%s] [%+v]", addr, err)
						//连不上，视为服务down掉，忽略
						break
					}
					client := pb.NewUserMsgIdSvrClient(conn)
					connData = grpcConn{Conn:conn, Client:client}
					addr2ConnMap[addr] = connData
				}
			}

			//
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := connData.Client.(pb.UserMsgIdSvrClient).PostMsg(ctx, &pb.PostMsgIdReq{UserId:userId, MsgId:msgId})
			cancel()
			if err != nil{
				rlog.Printf("PostMsgIdReq failed. addr=[%s] [%+v] userid=[%d] msgid=[%d]", addr, err, userId, msgId)
				//断开连接
				_ = connData.Conn.Close()
				delete(addr2ConnMap, addr)
				//重试一次
				time.Sleep(100*time.Millisecond)//等100ms
				continue
			}
			break
		}
	}

	return nil
}
func storeToDB(ctx context.Context, dbClient *dbClientT, content *pb.MsgData) error{
	//允许重试一次
	for i:=0; i<2; i++{
		if dbClient.conn == nil{
			var err interface{}
			dbClient.conn, err = grpc.Dial(gConfig.DBSvrAddr, grpc.WithInsecure())
			if err != nil {
				rlog.Printf("connect frontSvr failed [%+v]", err)
				break
			}
			dbClient.client = pb.NewDbSvrClient(dbClient.conn)
		}
		_, err := dbClient.client.Post(ctx, &pb.DBPostReq{Content:content})
		if err != nil{
			//重连一下
			_ = dbClient.conn.Close()
			dbClient.conn = nil
			continue
		}

		return nil
	}
	return errors.New("store to db failed")
}
//func storeContent(content *pb.MsgData, addr2ConnMap map[string]grpcConn) error{
//	msgId := content.MsgId
//
//	//根据msgId分片，找出所有对应的contentSvr
//	var contentSvrAddrArr []string
//	{
//		gContentSvrData.RLock()
//
//		index := msgId%int64(len(gContentSvrData.contentSvrArr))
//		//用值拷贝，防止引用用一个数组，gUserMsgIdSvrData中的数组是处于竞态环境
//		contentSvrAddrArr = make([]string, len(gContentSvrData.contentSvrArr[index].SvrAddrArr))
//		copy(contentSvrAddrArr, gContentSvrData.contentSvrArr[index].SvrAddrArr)
//
//		gContentSvrData.RUnlock()
//	}
//
//	//
//	for _, addr := range contentSvrAddrArr{
//		//允许重试一次
//		for i:=0; i<2; i++ {
//			//获取地址对应的链接
//			var connData grpcConn
//			{
//				var ok bool
//				connData, ok = addr2ConnMap[addr]
//				if !ok {
//					//还未链接则链接
//					conn, err := grpc.Dial(addr, grpc.WithInsecure())
//					if err != nil {
//						rlog.Printf("connect contentSvr failed. addr=[%s] [%+v]", addr, err)
//						//连不上，视为服务down掉，忽略
//						break
//					}
//					client := pb.NewContentSvrClient(conn)
//					connData = grpcConn{Conn: conn, Client: client}
//					addr2ConnMap[addr] = connData
//				}
//			}
//
//			//
//			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//			_, err := connData.Client.(pb.ContentSvrClient).PostMsg(ctx, &pb.PostMsgContentReq{Content: content})
//			cancel()
//			if err != nil {
//				rlog.Printf("contentSvr PostMsg failed. addr=[%s] [%+v] content=[%+v]", addr, err, content)
//				//断开连接
//				_ = connData.Conn.Close()
//				delete(addr2ConnMap, addr)
//				//重试一次
//				time.Sleep(100*time.Millisecond)//等100ms
//				continue
//			}
//		}
//	}
//
//	return nil
//}
func sendToPushSvr(userId int64, msgId int64, addr2ConnMap map[string]grpcConn) error{

	//先把svr地址拷贝出来
	gPushSvrData.Lock()
	//用值拷贝，防止引用用一个数组，gUserMsgIdSvrData中的数组是处于竞态环境
	svrAddrArr := make([]string, len(gPushSvrData.SvrAddrArr))
	copy(svrAddrArr, gPushSvrData.SvrAddrArr)
	startSvrIndex := gPushSvrData.curIndex
	gPushSvrData.curIndex++
	if gPushSvrData.curIndex >= len(gPushSvrData.SvrAddrArr){
		gPushSvrData.curIndex = 0
	}
	gPushSvrData.Unlock()

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
					conn, err := grpc.Dial(addr, grpc.WithInsecure())
					if err != nil {
						rlog.Printf("connect pushSvr failed. addr=[%s] [%+v]", addr, err)
						//连不上，视为服务down掉，忽略
						break
					}
					client := pb.NewPushSvrClient(conn)
					connData = grpcConn{Conn: conn, Client: client}
					addr2ConnMap[addr] = connData
				}
			}

			//
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := connData.Client.(pb.PushSvrClient).Push(ctx, &pb.PushReq{UserId:userId, MsgId:msgId})
			cancel()
			if err != nil {
				rlog.Printf("push failed. addr=[%s] [%+v] userId=%d, msgId=%d", addr, err, userId, msgId)
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

	return errors.New("no pushSvr available")
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
					gUserMsgIdSvrData.Lock()
					gUserMsgIdSvrData.UserMsgIdSvrArr = []userMsgIdSvrShareArr{}
					for _, arr := range config.UserMsgIdSvrArr{
						shareArr := userMsgIdSvrShareArr{}
						shareArr.SvrAddrArr = arr
						gUserMsgIdSvrData.UserMsgIdSvrArr = append(gUserMsgIdSvrData.UserMsgIdSvrArr, shareArr)
					}
					gUserMsgIdSvrData.Unlock()

					gContentSvrData.Lock()
					gContentSvrData.contentSvrArr = []contentShareArr{}
					for _, arr := range config.ContentSvrArr{
						shareArr := contentShareArr{}
						shareArr.SvrAddrArr = arr
						gContentSvrData.contentSvrArr = append(gContentSvrData.contentSvrArr, shareArr)
					}
					gContentSvrData.Unlock()

					gPushSvrData.Lock()
					gPushSvrData.SvrAddrArr = config.PushSvrAddrArr
					gPushSvrData.curIndex = 0
					gPushSvrData.Unlock()
				}
			}
		}

		time.Sleep(10 * time.Second)
	}
}