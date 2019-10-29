/*
@Author : Ryan.wuxiaoyong
*/

package main


import (

	"context"
	"encoding/json"
	"go.mongodb.org/mongo-driver/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"io/ioutil"
	"log"
	"net"
	"os"
	"time"

	"WeiBo/common/dbConnPool"
	"WeiBo/common/rlog"
)

//配置文件格式
type configType struct {
	ListenAddr string  `json: "listenAddr"`
	MongoDBUrl string `json: "mongoDBUrl"`
	MongoDBName string `json: "mongoDBName"`
}
//配置文件数据对象
var gConfig = &configType{}

//db连接池
var gDBConnPool = dbConnPool.MongoConnPool{}

func main(){
	if len(os.Args)!=2 {
		log.Fatal("xxx configPath")
	}
	confPath := os.Args[1]

	//
	confBytes, err := ioutil.ReadFile(confPath)
	if err != nil {
		log.Fatalf("Read config file failed.[%s][%+v]", confPath, err)
	}
	//解析
	err = json.Unmarshal(confBytes, gConfig)
	if err != nil {
		log.Fatalf("Read config file failed.[%s][%+v]", confPath, err)
	}

	//
	err = gDBConnPool.Init(100, gConfig.MongoDBUrl, "", "")
	if err != nil{
		log.Fatalf("%+v", err)
	}
	log.Printf("dbpool init successed")


	//开启grpc服务
	lis, err := net.Listen("tcp", gConfig.ListenAddr)
	if err != nil {
		log.Fatalf("failed to listen: %+v", err)
	}
	svrOpt := grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionIdle: 2*time.Minute})
	grpcServer := grpc.NewServer(svrOpt)
	rlog.RegisterRlogSvrServer(grpcServer, &serverT{})
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %+v", err)
	}else{
		log.Printf("begin to grpc serve")
	}

}

//gRpc proto///////////////////////////////////////////
type serverT struct{}
func (s *serverT) Log(ctx context.Context, req *rlog.LogReq) (*rlog.LogRsp, error) {
	log.Printf("[log] [%s] [%s] [time: %d]", req.MyName, req.Log, req.TimeStamp)

	client, err := gDBConnPool.WaitForOneConn(ctx)
	if err != nil{
		log.Printf("gDBConnPool.WaitForOneConn failed. err=[%+v]", err)
		return &rlog.LogRsp{}, err
	}
	defer gDBConnPool.ReturnConn(client)

	//插入
	data := bson.D{{"svr_name", req.MyName}, {"log", req.Log}, {"svr_time", req.TimeStamp}, {"lc_time", time.Now().Unix()}}
	collection := client.Database(gConfig.MongoDBName).Collection("svr_log")
	_, err = collection.InsertOne(ctx, data)
	if err != nil{
		log.Printf("insert log failed. err=[%+v]", err)
		return &rlog.LogRsp{}, err
	}

	return &rlog.LogRsp{}, nil
}
func (s *serverT) LogFatal(ctx context.Context, req *rlog.LogFatalReq) (*rlog.LogFatalRsp, error) {
	log.Printf("\n\n[!!!!!!!!!!Fatal!!!!!!!!!!] [%s] [%s] [time: %d]\n\n", req.MyName, req.Log, req.TimeStamp)

	client, err := gDBConnPool.WaitForOneConn(ctx)
	if err != nil{
		log.Printf("gDBConnPool.WaitForOneConn failed. err=[%+v]", err)
		return &rlog.LogFatalRsp{}, err
	}
	defer gDBConnPool.ReturnConn(client)

	//插入
	data := bson.D{{"svr_name", req.MyName}, {"log", req.Log}, {"svr_time", req.TimeStamp}, {"lc_time", time.Now().Unix()}}
	collection := client.Database(gConfig.MongoDBName).Collection("svr_fatal")
	_, err = collection.InsertOne(ctx, data)
	if err != nil{
		log.Printf("insert log failed. err=[%+v]", err)
		return &rlog.LogFatalRsp{}, err
	}

	return &rlog.LogFatalRsp{}, nil
}
func (s *serverT) StatPoint(ctx context.Context, req *rlog.StatPointReq) (*rlog.StatPointRsp, error) {
	client, err := gDBConnPool.WaitForOneConn(ctx)
	if err != nil{
		log.Printf("gDBConnPool.WaitForOneConn failed. err=[%+v]", err)
		return &rlog.StatPointRsp{}, err
	}
	defer gDBConnPool.ReturnConn(client)

	var dataArr []rlog.StatPointData
	for _, data := range req.DataArr{
		d := rlog.StatPointData{Name:data.Name, Data:data.Data}
		dataArr = append(dataArr, d)
	}

	//插入
	data := bson.D{{"svr_name", req.MyName}, {"id", req.PointId},
		{"data", dataArr},
		{"svr_time", req.TimeStamp}, {"lc_time", time.Now().Unix()}}
	collection := client.Database(gConfig.MongoDBName).Collection("svr_stat_point")
	_, err = collection.InsertOne(ctx, data)
	if err != nil{
		log.Printf("insert log failed. err=[%+v]", err)
		return &rlog.StatPointRsp{}, err
	}

	return &rlog.StatPointRsp{}, nil
}
func (s *serverT) Warning(ctx context.Context, req *rlog.WarningReq) (*rlog.WarningRsp, error) {
	var dataArr []rlog.StatPointData
	for _, data := range req.DataArr{
		d := rlog.StatPointData{Name:data.Name, Data:data.Data}
		dataArr = append(dataArr, d)
	}

	levStr := ""
	if req.Level == rlog.WarningLevelFatal{
		levStr = "[!!!!!Fatal!!!!!] "
	}
	log.Printf("\n\n[!!!!!!Warning!!!!!!] %s[%s] [%s] %v [time: %d]\n\n", levStr, req.MyName, req.Desc, dataArr, req.TimeStamp)

	client, err := gDBConnPool.WaitForOneConn(ctx)
	if err != nil{
		log.Printf("gDBConnPool.WaitForOneConn failed. err=[%+v]", err)
		return &rlog.WarningRsp{}, err
	}
	defer gDBConnPool.ReturnConn(client)

	//插入
	data := bson.D{{"svr_name", req.MyName}, {"level", req.Level},
		{"desc", req.Desc}, {"data", dataArr},
		{"svr_time", req.TimeStamp}, {"lc_time", time.Now().Unix()}}
	collection := client.Database(gConfig.MongoDBName).Collection("svr_warning")
	_, err = collection.InsertOne(ctx, data)
	if err != nil{
		log.Printf("insert log failed. err=[%+v]", err)
		return &rlog.WarningRsp{}, err
	}
	return &rlog.WarningRsp{}, nil
}

//gRpc proto end///////////////////////////////////////////



