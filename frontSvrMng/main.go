/*
@Author : Ryan.wuxiaoyong
*/

package main


import (
	"WeiBo/common"
	"context"
	"encoding/json"
	"google.golang.org/grpc/keepalive"
	"io/ioutil"
	"sync/atomic"

	//"log"
	"net"
	"sync"
	"time"

	//"time"
	"os"
	"google.golang.org/grpc"
	"errors"

	pb "WeiBo/common/protobuf"
	sclBrk "WeiBo/common/scaleAndBreak"
	"WeiBo/common/rlog"
)

//配置文件格式
type configType struct {
	MyName string `json: "myName"`
	RlogSvrAddr string `json: "rlogSvrAddr"`
	ListenAddr string  `json: "listenAddr"`
	SvrAddrArr []string `json: "svrAddrArr"`
}
//配置文件数据对象
var gConfig = &configType{}

//有效服务
var gAvailSvrGroup *sclBrk.AvailSvrGroup = nil

//当前轮询有效服务序号
type syncNum struct {
	sync.Mutex

	num int
}
var gCurAvailSvrIndex = syncNum{sync.Mutex{}, 0}

//req的计数器，为线程安全请用atomic.AddUint64()进行操作
var gReqQueryFrontSvrCounter int64
//rsp计数器，为线程安全请用atomic.AddUint64()进行操作
var gRspQueryFrontSvrCounter int64

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

	//开启缩扩容与熔断机制
	gAvailSvrGroup, err = sclBrk.Start(confPath, 2, serviceCheck)
	if err != nil{
		rlog.Fatalf("start expanseBreak failed.[%+v]", err)
	}

	//开启数据打点routine
	go statReportRoutine()

	//开启grpc服务
	lis, err := net.Listen("tcp", gConfig.ListenAddr)
	if err != nil {
		rlog.Fatalf("failed to listen: %+v", err)
	}
	rlog.Printf("begin to Listen [%s]", gConfig.ListenAddr)
	svrOpt := grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionIdle: (common.ClientMaxIdleSec+5)*time.Second})
	grpcServer := grpc.NewServer(svrOpt)
	pb.RegisterFrontSvrMngServer(grpcServer, &serverT{})
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
			data := rlog.StatPointData{Name:"reqQueryFrontSvrCount", Data:gReqQueryFrontSvrCounter}
			dataArr = append(dataArr, data)
			data = rlog.StatPointData{Name:"rspQueryFrontSvrCount", Data:gRspQueryFrontSvrCounter}
			dataArr = append(dataArr, data)
		}
		rlog.StatPoint("totalReport", dataArr)
	}
}

//gRpc proto///////////////////////////////////////////
type serverT struct{}

func (s *serverT) QueryMyFrontSvr(ctx context.Context, in *pb.CClientAuthInfo) (*pb.CFrontSvrAddr, error) {
	//统计计数
	atomic.AddInt64(&gReqQueryFrontSvrCounter, 1)

	svrAddr := ""

	//这里负载均衡是很简单的均摊方法。
	//如果需要能根据实时负载进行均衡的话，可以增加负载探知能力。
	{
		gCurAvailSvrIndex.Lock()
		gAvailSvrGroup.RLock()

		gCurAvailSvrIndex.num++
		if len(gAvailSvrGroup.SvrArr) > 0{
			if gCurAvailSvrIndex.num < len(gAvailSvrGroup.SvrArr) {
				svrAddr = gAvailSvrGroup.SvrArr[gCurAvailSvrIndex.num]
			}else{
				gCurAvailSvrIndex.num = 0
				svrAddr = gAvailSvrGroup.SvrArr[gCurAvailSvrIndex.num]
			}
		}
		//rlog.Printf("svrAddr=[%s] index=[%d] svrArr=%+v", svrAddr, gCurAvailSvrIndex.num, gAvailSvrGroup.SvrArr)

		gAvailSvrGroup.RUnlock()
		gCurAvailSvrIndex.Unlock()
	}

	if svrAddr == "" {
		return &pb.CFrontSvrAddr{Addr:""}, errors.New("no available server")
	}

	//统计计数
	atomic.AddInt64(&gRspQueryFrontSvrCounter, 1)
	return &pb.CFrontSvrAddr{Addr:svrAddr}, nil
}
//gRpc proto end///////////////////////////////////////////

//检查服务是否可用，如果返回非nil则会导致暂时熔断
func serviceCheck(svrAddr string) error{
	conn, err := grpc.Dial(svrAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewFrontSvrClient(conn)

	_, err = client.CheckAvail(context.TODO(), &pb.FrontCheckAvailReq{})
	return err
}

