/*
@Author : Ryan.wuxiaoyong
*/

package main

import (
	"WeiBo/common"
	"context"
	"encoding/json"
	"google.golang.org/grpc/keepalive"
	"math/rand"
	"sort"
	"time"

	"errors"
	"google.golang.org/grpc"
	"io"

	//"go/types"
	"io/ioutil"
	//"log"
	"net"
	"sync"
	//"time"
	"os"

	//"WeiBo/common"
	pb "WeiBo/common/protobuf"
	"WeiBo/common/rlog"
)

//配置文件格式
type configType struct {
	MyName string `json: "myName"`
	RlogSvrAddr string `json: "rlogSvrAddr"`
	ListenAddr string  `json: "listenAddr"`
	FollowSvrAddr string `json: "followSvrAddr"`
}
//配置文件数据对象
var gConfig = &configType{}

type shareSvrArrConfigT struct {
	UserMsgIdSvrArr [][]string `json: "userMsgIdSvrArr"`
	ContentSvrArr [][]string `json: "contentSvrArr"`
}

//userMsgIdSvr分片数据/////////////////////////////////////////
type userMsgIdSvrDataT struct {
	sync.RWMutex

	UserMsgIdSvrArr []userMsgIdSvrShareArr //分片服务地址列表
}
type userMsgIdSvrShareArr struct{
	SvrAddrArr []string //平行负荷分担，轮询

	curIndex int //当前轮询index
}
//userMsgIdSvr分片数据对象
var gUserMsgIdSvrData = userMsgIdSvrDataT{}


//contentSvr分片数据/////////////////////////////////////////
type contentSvrDataT struct {
	sync.RWMutex

	contentSvrArr []contentShareArr //分片服务地址列表
}
type contentShareArr struct{
	SvrAddrArr []string //平行负荷分担，轮询

	curIndex int //当前轮询index
}
//contentSvr分片数据对象
var gContentSvrData = contentSvrDataT{}



//pull请求内部输送管道，所有请求都汇聚到这个管道
type pullReqPack struct {
	RspChn chan *pb.PullRsp
	Req *pb.PullReq
}
var gPullReqChn = make(chan *pullReqPack, 10000)


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

	//启动pull组装routine群
	for i:=0; i<1000; i++{
		go pullAssembleRoutine()
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
	pb.RegisterPullSvrServer(grpcServer, &serverT{})
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
			data := rlog.StatPointData{Name:"reqChanLen", Data:int64(len(gPullReqChn))}
			dataArr = append(dataArr, data)
		}
		rlog.StatPoint("totalReport", dataArr)
	}
}

//gRpc proto///////////////////////////////////////////
type serverT struct{}

func (s *serverT) CreateStream(stream pb.PullSvr_CreateStreamServer) error {
	//创建rsp接受管道
	rspChn := make(chan *pb.PullRsp, 1000)
	//独立一个routine处理回复
	reqEofSyncChn := make(chan int, 1)
	noMoreRspSyncChn := make(chan int, 1)
	go pullRspRecvRoutine(rspChn, stream, reqEofSyncChn, noMoreRspSyncChn)

	for {
		req, err := stream.Recv()
		if err == io.EOF{
			//对方不再发req了
			rlog.Printf("pullSvr recv req EOF")
			break
		}
		if err != nil{
			rlog.Printf("pullSvr recv err.[%+v]", err)
			break
		}

		if req.FrontReqId == 0 {
			//如果是心跳包（空包）则直接返回
			rspChn<- &pb.PullRsp{}
		}else{
			//
			reqPack := &pullReqPack{RspChn:rspChn, Req:req}
			gPullReqChn<- reqPack
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
func pullRspRecvRoutine(chn chan *pb.PullRsp, stream pb.PullSvr_CreateStreamServer, reqEofSyncChn chan int, noMoreRspSyncChn chan int){
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

func pullAssembleRoutine(){

	//服务器地址到链接的map
	addr2ConnMap := make(map[string]grpcConn)

	for{
		reqPack := <-gPullReqChn

		//先查询所有关注列表
		followIdArr, err := getFollowUserIdArr(reqPack.Req.UserId, addr2ConnMap)
		if err != nil {
			//丢弃
			continue
		}

		//
		msgArr, err := queryMsgAndAssemble(followIdArr, reqPack.Req.LastMsgId, addr2ConnMap)
		if err != nil {
			//丢弃
			continue
		}
		rsp := pb.PullRsp{FrontReqId:reqPack.Req.FrontReqId, MsgArr:msgArr}
		//rsp := pb.PullRsp{FrontReqId:reqPack.Req.FrontReqId, MsgArr:[]*pb.MsgData{{UserId: 12345, Text: "hello test text1", ImgUrlArr: []string{}, VideoUrl: ""},
		//	{UserId: 23456, Text: "hello test text2", ImgUrlArr: []string{}, VideoUrl: ""}}}
		//rlog.Printf("PullRsp %+v", rsp)

		reqPack.RspChn<- &rsp
	}
}

func getFollowUserIdArr(userId int64, addr2ConnMap map[string]grpcConn) (*[]int64, error){
	//允许重试一次
	for i:=0; i<2; i++{
		//获取地址对应的链接
		var connData grpcConn
		{
			var ok bool
			connData, ok = addr2ConnMap[gConfig.FollowSvrAddr]
			if !ok{
				//还未链接则链接
				conn, err := grpc.Dial(gConfig.FollowSvrAddr, grpc.WithInsecure())
				if err != nil {
					rlog.Printf("connect FollowSvr failed. [%+v]", err)
					//连不上，视为服务down掉，忽略
					return &[]int64{}, errors.New("connect FollowSvr failed")
				}
				client := pb.NewFollowSvrClient(conn)
				connData = grpcConn{Conn:conn, Client:client}
				addr2ConnMap[gConfig.FollowSvrAddr] = connData
			}
		}

		//
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		rsp, err := connData.Client.(pb.FollowSvrClient).QueryFollowList(ctx, &pb.QueryFollowListReq{UserId:userId})
		cancel()
		if err != nil {
			rlog.Printf("QueryFollowList failed. [%+v]", err)
			_ = connData.Conn.Close()
			delete(addr2ConnMap, gConfig.FollowSvrAddr)
			//重试一次
			continue
		}
		//rlog.Printf("QueryFollowList %+v", rsp)
		return &rsp.FollowIdArr, nil
	}

	return &[]int64{}, errors.New("QueryFollowList failed")
}

//获取地址对应的链接，如果还未链接则链接
type grpcConn struct {
	Conn *grpc.ClientConn
	Client interface{}
}
func queryMsgAndAssemble(followIdArr *[]int64, lastMsgId int64, addr2ConnMap map[string]grpcConn) ([]*pb.MsgData, error) {
	//组装pull结果msgId
	msgIdArr := assembleMsgId(followIdArr, lastMsgId, addr2ConnMap)

	//取msg内容
	msgDataArr := assembleMsgContent(msgIdArr, addr2ConnMap)

	return msgDataArr, nil
}
func assembleMsgId(followIdArr *[]int64, lastMsgId int64, addr2ConnMap map[string]grpcConn) []int64{
	//根据userId分片，寻找userMsgSvr，并将userId聚合在归宿svr下面。
	userSvrAddrMap := make(map[string][]int64)
	//把svr对应的负荷分担svr全部记录下来，后面如果链接出现故障就用备用的
	svrBackAddrMap := make(map[string][]string)
	{
		gUserMsgIdSvrData.Lock()

		//哪些svr被用到了，做一下记录
		usedSvrIndexMap := make(map[int64]bool)
		for _, userId := range *followIdArr{
			index := userId%int64(len(gUserMsgIdSvrData.UserMsgIdSvrArr))
			usedSvrIndexMap[index] = true

			userSvrAddrMap[gUserMsgIdSvrData.UserMsgIdSvrArr[index].SvrAddrArr[gUserMsgIdSvrData.UserMsgIdSvrArr[index].curIndex]] =
				append(userSvrAddrMap[gUserMsgIdSvrData.UserMsgIdSvrArr[index].SvrAddrArr[gUserMsgIdSvrData.UserMsgIdSvrArr[index].curIndex]], userId)
		}
		//
		for index := range usedSvrIndexMap{
			//把svr对应的负荷分担svr全部记录下来
			curAddr := gUserMsgIdSvrData.UserMsgIdSvrArr[index].SvrAddrArr[gUserMsgIdSvrData.UserMsgIdSvrArr[index].curIndex]
			svrBackAddrMap[curAddr] = make([]string, len(gUserMsgIdSvrData.UserMsgIdSvrArr[index].SvrAddrArr))
			//用值拷贝，防止引用用一个数组，gUserMsgIdSvrData中的数组是处于竞态环境
			copy(svrBackAddrMap[curAddr], gUserMsgIdSvrData.UserMsgIdSvrArr[index].SvrAddrArr)
			//rlog.Printf("org:%v back:%v", gUserMsgIdSvrData.UserMsgIdSvrArr[index].SvrAddrArr, svrBackAddrMap[curAddr])

			//轮询+1
			gUserMsgIdSvrData.UserMsgIdSvrArr[index].curIndex++
			if gUserMsgIdSvrData.UserMsgIdSvrArr[index].curIndex >= len(gUserMsgIdSvrData.UserMsgIdSvrArr[index].SvrAddrArr) {
				gUserMsgIdSvrData.UserMsgIdSvrArr[index].curIndex = 0
			}
		}

		gUserMsgIdSvrData.Unlock()
	}

	//
	var msgIdArr []int64
	for orgAddr, userIdArr := range userSvrAddrMap{
		//获取地址对应的链接
		var connData grpcConn
		var actSvrAddr = orgAddr

		//debug
		var tempAddrArr = make([]string, len(svrBackAddrMap[orgAddr]))
		copy(tempAddrArr, svrBackAddrMap[orgAddr])

		tryAnotherSvr:
		for len(svrBackAddrMap[orgAddr]) > 0{
			if actSvrAddr == ""{
				//当前指定的svr不可用了，随机选一个
				randIndex := rand.Intn(len(svrBackAddrMap[orgAddr]))
				actSvrAddr = svrBackAddrMap[orgAddr][randIndex]
			}
			//rlog.Printf("orgAddr=[%s] actSvrAddr=[%s] svrBackAddrMap[orgAddr]=%v", orgAddr, actSvrAddr, svrBackAddrMap[orgAddr])

			var ok bool
			connData, ok = addr2ConnMap[actSvrAddr]
			if !ok{
				//还未链接则链接
				conn, err := grpc.Dial(actSvrAddr, grpc.WithInsecure())
				if err != nil {
					rlog.Printf("connect userIdMsgSvr failed. actSvrAddr=[%s] [%+v]", actSvrAddr, err)
					//当前不可用，换下一个备用的
					//把当前的svr删掉
					for i, addr := range svrBackAddrMap[orgAddr]{
						if addr == actSvrAddr{
							svrBackAddrMap[orgAddr][i] =  svrBackAddrMap[orgAddr][len( svrBackAddrMap[orgAddr])-1]
							svrBackAddrMap[orgAddr] =  svrBackAddrMap[orgAddr][:len( svrBackAddrMap[orgAddr])-1]
						}
					}
					actSvrAddr = ""

					goto tryAnotherSvr
				}
				client := pb.NewUserMsgIdSvrClient(conn)
				connData = grpcConn{Conn:conn, Client:client}
				addr2ConnMap[actSvrAddr] = connData
			}
			break
		}

		if len(svrBackAddrMap[orgAddr]) == 0 {
			//所有svr都不可用，只有丢弃
			rlog.Printf("all userIdMsgSvr failed. orgAddr=[%s] tempAddrArr=%v", orgAddr, tempAddrArr)
			continue
		}

		//
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		rsp, err := connData.Client.(pb.UserMsgIdSvrClient).PullMsg(ctx, &pb.PullMsgIdReq{UserIdArr:userIdArr, LastMsgId:lastMsgId})
		cancel()
		if err != nil{
			rlog.Printf("PullMsg failed. addr=[%s] [%+v]", actSvrAddr, err)
			//断开连接
			_ = connData.Conn.Close()
			delete(addr2ConnMap, actSvrAddr)
			//当前不可用，换下一个备用的
			//把当前的svr删掉
			for i, addr := range svrBackAddrMap[orgAddr]{
				if addr == actSvrAddr{
					svrBackAddrMap[orgAddr][i] =  svrBackAddrMap[orgAddr][len( svrBackAddrMap[orgAddr])-1]
					svrBackAddrMap[orgAddr] =  svrBackAddrMap[orgAddr][:len( svrBackAddrMap[orgAddr])-1]
				}
			}
			actSvrAddr = ""

			goto tryAnotherSvr
		}
		msgIdArr = append(msgIdArr, rsp.MsgIdArr...)
	}
	//将msgIdArr按照msgId从小到大排序
	sort.Slice(msgIdArr, func(i, j int)bool{return msgIdArr[i] < msgIdArr[j]})
	//选id最大的xx条
	if len(msgIdArr)-common.MaxMsgCountInPull > 0 {
		msgIdArr = msgIdArr[len(msgIdArr)-common.MaxMsgCountInPull:]
	}

	return msgIdArr
}
func assembleMsgContent(msgIdArr []int64, addr2ConnMap map[string]grpcConn)[]*pb.MsgData{
	//根据msgId分片，寻找contentSvr，并将msgId聚合在归宿svr下面。
	contentSvrAddrMap := make(map[string][]int64)
	//把svr对应的负荷分担svr全部记录下来，后面如果链接出现故障就用备用的
	svrBackAddrMap := make(map[string][]string)
	{
		gContentSvrData.Lock()

		//哪些svr被用到了，做一下记录
		usedSvrIndexMap := make(map[int64]bool)
		for _, msgId := range msgIdArr{
			index := msgId%int64(len(gContentSvrData.contentSvrArr))
			usedSvrIndexMap[index] = true

			contentSvrAddrMap[gContentSvrData.contentSvrArr[index].SvrAddrArr[gContentSvrData.contentSvrArr[index].curIndex]] =
				append(contentSvrAddrMap[gContentSvrData.contentSvrArr[index].SvrAddrArr[gContentSvrData.contentSvrArr[index].curIndex]], msgId)
		}

		for index := range usedSvrIndexMap{
			//把svr对应的负荷分担svr全部记录下来
			curAddr := gContentSvrData.contentSvrArr[index].SvrAddrArr[gContentSvrData.contentSvrArr[index].curIndex]
			svrBackAddrMap[curAddr] = make([]string, len(gContentSvrData.contentSvrArr[index].SvrAddrArr))
			//用值拷贝，防止引用用一个数组，gUserMsgIdSvrData中的数组是处于竞态环境
			copy(svrBackAddrMap[curAddr], gContentSvrData.contentSvrArr[index].SvrAddrArr)
			//rlog.Printf("org:%v back:%v", gContentSvrData.contentSvrArr[index].SvrAddrArr, svrBackAddrMap[curAddr])

			//轮询+1
			gContentSvrData.contentSvrArr[index].curIndex++
			if gContentSvrData.contentSvrArr[index].curIndex >= len(gContentSvrData.contentSvrArr[index].SvrAddrArr) {
				gContentSvrData.contentSvrArr[index].curIndex = 0
			}
		}

		gContentSvrData.Unlock()
	}

	//
	var msgDataArr []*pb.MsgData
	for orgAddr, msgIdArr := range contentSvrAddrMap{
		//获取地址对应的链接
		var connData grpcConn
		var actSvrAddr = orgAddr

		////debug
		//var tempAddrArr = make([]string, len(svrBackAddrMap[orgAddr]))
		//copy(tempAddrArr, svrBackAddrMap[orgAddr])
		//if len(svrBackAddrMap[orgAddr]) == 0 {
		//	rlog.Printf("svrBackAddrMap[%s] empty. ", orgAddr)
		//}

	tryAnotherSvr:
		for len(svrBackAddrMap[orgAddr]) > 0{
			if actSvrAddr == ""{
				//当前指定的svr不可用了，随机选一个
				randIndex := rand.Intn(len(svrBackAddrMap[orgAddr]))
				actSvrAddr = svrBackAddrMap[orgAddr][randIndex]
			}
			//rlog.Printf("orgAddr=[%s] actSvrAddr=[%s] svrBackAddrMap[orgAddr]=%v", orgAddr, actSvrAddr, svrBackAddrMap[orgAddr])

			var ok bool
			connData, ok = addr2ConnMap[actSvrAddr]
			if !ok{
				//还未链接则链接
				conn, err := grpc.Dial(actSvrAddr, grpc.WithInsecure())
				if err != nil {
					rlog.Printf("connect contentSvr failed. actSvrAddr=[%s] [%+v]", actSvrAddr, err)
					//当前不可用，换下一个备用的
					//把当前的svr删掉
					for i, addr := range svrBackAddrMap[orgAddr]{
						if addr == actSvrAddr{
							svrBackAddrMap[orgAddr][i] =  svrBackAddrMap[orgAddr][len( svrBackAddrMap[orgAddr])-1]
							svrBackAddrMap[orgAddr] =  svrBackAddrMap[orgAddr][:len( svrBackAddrMap[orgAddr])-1]
						}
					}
					actSvrAddr = ""

					goto tryAnotherSvr
				}
				client := pb.NewContentSvrClient(conn)
				connData = grpcConn{Conn:conn, Client:client}
				addr2ConnMap[actSvrAddr] = connData
			}
			break
		}

		if len(svrBackAddrMap[orgAddr]) == 0 {
			//所有svr都不可用，只有丢弃
			rlog.Printf("all contentSvr failed. orgAddr=[%s]", orgAddr)
			continue
		}

		////获取地址对应的链接
		//connData, ok := addr2ConnMap[addr]
		//if !ok{
		//	//还未链接则链接
		//	conn, err := grpc.Dial(addr, grpc.WithInsecure())
		//	if err != nil {
		//		log.Printf("connect contentSvr failed. addr=[%s] [%+v]", addr, err)
		//		//丢弃
		//		continue
		//	}
		//	client := pb.NewContentSvrClient(conn)
		//	connData = grpcConn{Conn:conn, Client:client}
		//	addr2ConnMap[addr] = connData
		//}

		//
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		rsp, err := connData.Client.(pb.ContentSvrClient).PullMsg(ctx, &pb.PullMsgContentReq{MsgIdArr:msgIdArr})
		cancel()
		if err != nil{
			rlog.Printf("PullMsg failed. addr=[%s] [%+v]", actSvrAddr, err)
			//断开连接
			_ = connData.Conn.Close()
			delete(addr2ConnMap, actSvrAddr)
			//当前不可用，换下一个备用的
			//把当前的svr删掉
			for i, addr := range svrBackAddrMap[orgAddr]{
				if addr == actSvrAddr{
					svrBackAddrMap[orgAddr][i] =  svrBackAddrMap[orgAddr][len( svrBackAddrMap[orgAddr])-1]
					svrBackAddrMap[orgAddr] =  svrBackAddrMap[orgAddr][:len( svrBackAddrMap[orgAddr])-1]
				}
			}
			actSvrAddr = ""

			goto tryAnotherSvr
		}
		msgDataArr = append(msgDataArr, rsp.MsgArr...)
	}

	sort.Slice(msgDataArr, func(i, j int)bool{return msgDataArr[i].MsgId < msgDataArr[j].MsgId})

	return msgDataArr
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
				}
			}
		}

		time.Sleep(10 * time.Second)
	}
}