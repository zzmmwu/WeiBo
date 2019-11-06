/*
@Author : Ryan.wuxiaoyong
*/

package main


import (
	"context"
	"encoding/json"
	"errors"
	"google.golang.org/grpc"
	"io"
	"io/ioutil"
	"net"
	"os"
	"sync/atomic"
	"time"

	pb "WeiBo/common/protobuf"
	"WeiBo/common/rlog"
)

//配置文件格式
type configType struct {
	MyName string `json: "myName"`
	RlogSvrAddr string `json: "rlogSvrAddr"`
	ListenAddr string  `json: "listenAddr"`
	FollowSvrAddr string `json: "followSvrAddr"`
	FollowedSvrAddr string `json: "followedSvrAddr"`
	DBSvrAddr string `json: "dbSvrAddr"`
}
//配置文件数据对象
var gConfig = &configType{}

//req的计数器，为线程安全请用atomic.AddUint64()进行操作
var gReqFollowCounter int64
var gReqUnFollowCounter int64
//rsp计数器，为线程安全请用atomic.AddUint64()进行操作
var gRspFollowCounter int64
var gRspUnFollowCounter int64



//follow/unfollow请求内部输送管道，所有请求都汇聚到这个管道
type reqPack struct {
	RspChn chan *pb.RelationChgRsp
	Req *pb.RelationChgReq
}
var gReqChn = make(chan *reqPack, 10000)

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

	//
	for i:=0; i<100; i++{
		go procRoutine()
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
	grpcServer := grpc.NewServer()
	pb.RegisterRelationChangeSvrServer(grpcServer, &serverT{})
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
			data := rlog.StatPointData{Name:"reqFollowCount", Data:gReqFollowCounter}
			dataArr = append(dataArr, data)
			data = rlog.StatPointData{Name:"reqUnFollowCount", Data:gReqUnFollowCounter}
			dataArr = append(dataArr, data)

			data = rlog.StatPointData{Name:"rspFollowCount", Data:gRspFollowCounter}
			dataArr = append(dataArr, data)
			data = rlog.StatPointData{Name:"rspUnFollowCount", Data:gRspUnFollowCounter}
			dataArr = append(dataArr, data)

			data = rlog.StatPointData{Name:"reqChanLen", Data:int64(len(gReqChn))}
			dataArr = append(dataArr, data)
		}
		rlog.StatPoint("totalReport", dataArr)
	}
}

//gRpc proto///////////////////////////////////////////
type serverT struct{}

func (s *serverT) CreateStream(stream pb.RelationChangeSvr_CreateStreamServer) error {
	//创建rsp接受管道
	rspChn := make(chan *pb.RelationChgRsp, 1000)
	//独立一个routine处理回复
	reqEofSyncChn := make(chan int, 1)
	noMoreRspSyncChn := make(chan int, 1)
	go relChgRspRecvRoutine(rspChn, stream, reqEofSyncChn, noMoreRspSyncChn)

	for {
		req, err := stream.Recv()
		if err == io.EOF{
			//对方不再发req了
			rlog.Printf("relationChangeSvr recv req EOF")
			break
		}
		if err != nil{
			rlog.Printf("relationChangeSvr recv err.[%+v]", err)
			break
		}

		if req.FrontReqId == 0 {
			//如果是心跳包（空包）则直接返回
			rspChn<- &pb.RelationChgRsp{}
		}else{
			//
			reqPack := &reqPack{RspChn:rspChn, Req:req}
			gReqChn<- reqPack
		}
	}

	//
	reqEofSyncChn<- 1
	//等rsp接受routine结束
	<-noMoreRspSyncChn
	return nil
}


//gRpc proto end///////////////////////////////////////////

type grpcConn struct {
	Conn *grpc.ClientConn
	Client interface{}
}
var gFollowSvrConn = grpcConn{}
var gFollowedSvrConn = grpcConn{}
func callFollowSvr(ctx context.Context, followNotUnFollow bool, userId int64, followedId int64, svrConn *grpcConn) error{
	//允许重试一次
	for i:=0; i<2; i++{
		if svrConn.Conn == nil{
			//还未链接则链接
			conn, err := grpc.Dial(gConfig.FollowSvrAddr, grpc.WithInsecure())
			if err != nil {
				rlog.Printf("connect FollowSvr failed. [%+v]", err)
				//连不上，视为服务down掉，忽略
				return errors.New("connect FollowSvr failed")
			}
			client := pb.NewFollowSvrClient(conn)
			svrConn.Conn = conn
			svrConn.Client = client
		}

		//
		err := errors.New("")
		if followNotUnFollow{
			_, err = svrConn.Client.(pb.FollowSvrClient).Follow(ctx, &pb.FollowReq{UserId:userId, FollowedUserId:followedId})
		}else{
			_, err = svrConn.Client.(pb.FollowSvrClient).UnFollow(ctx, &pb.UnFollowReq{UserId:userId, UnFollowedUserId:followedId})
		}

		if err != nil {
			rlog.Printf("callFollowSvr failed. [%+v]", err)

			_ = svrConn.Conn.Close()
			svrConn.Conn = nil
			//重试一次
			continue
		}
		return nil
	}

	return errors.New("callFollowSvr failed")
}
func callFollowedSvr(ctx context.Context, followNotUnFollow bool, userId int64, followedId int64, svrConn *grpcConn) error{
	//允许重试一次
	for i:=0; i<2; i++{
		if svrConn.Conn == nil{
			//还未链接则链接
			conn, err := grpc.Dial(gConfig.FollowedSvrAddr, grpc.WithInsecure())
			if err != nil {
				rlog.Printf("connect FollowSvr failed. [%+v]", err)
				//连不上，视为服务down掉，忽略
				return errors.New("connect FollowSvr failed")
			}
			client := pb.NewFollowedSvrClient(conn)
			svrConn.Conn = conn
			svrConn.Client = client
		}

		//
		err := errors.New("")
		if followNotUnFollow{
			_, err = svrConn.Client.(pb.FollowedSvrClient).Follow(ctx, &pb.FollowReq{UserId:userId, FollowedUserId:followedId})
		}else{
			_, err = svrConn.Client.(pb.FollowedSvrClient).UnFollow(ctx, &pb.UnFollowReq{UserId:userId, UnFollowedUserId:followedId})
		}

		if err != nil {
			rlog.Printf("callFollowedSvr failed. [%+v]", err)

			_ = svrConn.Conn.Close()
			svrConn.Conn = nil
			//重试一次
			continue
		}
		return nil
	}

	return errors.New("callFollowedSvr failed")
}


func Follow(ctx context.Context, dbSvrConn *grpc.ClientConn, dbClient pb.DbSvrClient, userId int64, followId int64) error {
	//统计计数
	atomic.AddInt64(&gReqFollowCounter, 1)

	//rlog.Printf("Follow. in=[%+v]", in)
	//DB
	dbSuc := false
	{
		//允许重试一次
		for i:=0; i<2; i++{
			if dbSvrConn == nil{
				var err interface{}
				dbSvrConn, err = grpc.Dial(gConfig.DBSvrAddr, grpc.WithInsecure())
				if err != nil {
					rlog.Printf("connect frontSvr failed [%+v]", err)
					break
				}
				dbClient = pb.NewDbSvrClient(dbSvrConn)
			}
			_, err := dbClient.Follow(ctx, &pb.DBFollowReq{UserId:userId, FollowId:followId})
			if err != nil{
				//重连一下
				_ = dbSvrConn.Close()
				dbSvrConn = nil
				continue
			}

			dbSuc = true
		}
	}
	if !dbSuc{
		return errors.New("db failed")
	}

	//
	err := callFollowedSvr(ctx, true, userId, followId, &gFollowedSvrConn)
	if err != nil{
		rlog.Printf("callFollowedSvr failed. userid=%d followedId=%d. err=[%+v]", userId, followId, err)
		return err
	}
	//
	err = callFollowSvr(ctx, true, userId, followId, &gFollowSvrConn)
	if err != nil{
		rlog.Printf("callFollowSvr failed. userid=%d followedId=%d. err=[%+v]", userId, followId, err)
		return err
	}

	//统计计数
	atomic.AddInt64(&gRspFollowCounter, 1)
	return nil
}
func UnFollow(ctx context.Context, dbSvrConn *grpc.ClientConn, dbClient pb.DbSvrClient,userId int64, unFollowId int64) error {
	//统计计数
	atomic.AddInt64(&gReqUnFollowCounter, 1)

	//DB
	dbSuc := false
	{
		//允许重试一次
		for i:=0; i<2; i++{
			if dbSvrConn == nil{
				var err interface{}
				dbSvrConn, err = grpc.Dial(gConfig.DBSvrAddr, grpc.WithInsecure())
				if err != nil {
					rlog.Printf("connect frontSvr failed [%+v]", err)
					break
				}
				dbClient = pb.NewDbSvrClient(dbSvrConn)
			}
			_, err := dbClient.UnFollow(ctx, &pb.DBUnFollowReq{UserId:userId, FollowId:unFollowId})
			if err != nil{
				//重连一下
				_ = dbSvrConn.Close()
				dbSvrConn = nil
				continue
			}

			dbSuc = true
		}
	}
	if !dbSuc{
		return errors.New("db failed")
	}

	//
	err := callFollowedSvr(ctx, false, userId, unFollowId, &gFollowedSvrConn)
	if err != nil{
		rlog.Printf("callFollowedSvr failed. userid=%d followedId=%d. err=[%+v]", userId, unFollowId, err)
		return err
	}
	//
	err = callFollowSvr(ctx, false, userId, unFollowId, &gFollowSvrConn)
	if err != nil{
		rlog.Printf("callFollowSvr failed. userid=%d followedId=%d. err=[%+v]", userId, unFollowId, err)
		return err
	}

	//统计计数
	atomic.AddInt64(&gRspUnFollowCounter, 1)
	return nil
}

func procRoutine(){

	var dbSvrConn *grpc.ClientConn
	var dbClient pb.DbSvrClient

	for{
		reqPack := <-gReqChn

		if reqPack.Req.Follow{
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			err := Follow(ctx, dbSvrConn, dbClient, reqPack.Req.UserId, reqPack.Req.FollowedUserId)
			cancel()
			if err != nil{
				//丢弃，让请求自己超时
				continue
			}
		}else{
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			err := UnFollow(ctx,dbSvrConn, dbClient, reqPack.Req.UserId, reqPack.Req.FollowedUserId)
			cancel()
			if err != nil{
				//丢弃，让请求自己超时
				continue
			}
		}

		rsp := pb.RelationChgRsp{FrontReqId:reqPack.Req.FrontReqId}
		reqPack.RspChn<- &rsp
	}
}


//上如果30秒没有rsp了，且reqStream已经EOF则通知req routine
func relChgRspRecvRoutine(chn chan *pb.RelationChgRsp, stream pb.RelationChangeSvr_CreateStreamServer, reqEofSyncChn chan int, noMoreRspSyncChn chan int){
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
				rlog.Printf("relationChangeSvr send err.[%+v]", err)
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


