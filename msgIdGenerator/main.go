/*
@Author : Ryan.wuxiaoyong
*/

package main


import (
	"WeiBo/common"
	"context"
	"encoding/json"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io/ioutil"
	//"log"
	"net"
	"sync"
	"time"

	//"time"

	//"errors"
	"google.golang.org/grpc"
	//"time"
	"os"

	pb "WeiBo/common/protobuf"
	"WeiBo/common/rlog"
)

//配置文件格式
type configType struct {
	MyName string `json: "myName"`
	RlogSvrAddr string `json: "rlogSvrAddr"`
	ListenAddr string  `json: "listenAddr"`
	MongoDBUrl string `json: "mongoDBUrl"`
	MongoDBName string `json: "mongoDBName"`
}
//配置文件数据对象
var gConfig = &configType{}

//当前轮询有效服务序号
type msgIdGen struct {
	sync.Mutex

	lastMsgId int64
}
var gMsgIdGen = msgIdGen{sync.Mutex{}, 0}

var gDBClient *mongo.Client

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

	//链接数据库
	mongoOpt := options.Client().ApplyURI("mongodb://" + gConfig.MongoDBUrl)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	gDBClient, err = mongo.Connect(ctx, mongoOpt)
	cancel()
	if err != nil {
		rlog.Fatalf("db connect failed. %+v", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	err = gDBClient.Ping(ctx, nil)
	cancel()
	if err != nil {
		rlog.Fatalf("db connect failed. %+v", err)
	}
	gMsgIdGen.lastMsgId, err = getLastMsgIdFromDB()
	if err != nil{
		rlog.Fatalf("getLastMsgIdFromDB failed. err=[%+v]", err)
	}

	//开启grpc服务
	lis, err := net.Listen("tcp", gConfig.ListenAddr)
	if err != nil {
		rlog.Fatalf("failed to listen: %+v", err)
	}
	rlog.Printf("begin to Listen [%s]", gConfig.ListenAddr)
	grpcServer := grpc.NewServer()
	pb.RegisterMsgIdGeneratorServer(grpcServer, &serverT{})
	if err := grpcServer.Serve(lis); err != nil {
		rlog.Fatalf("failed to serve: %+v", err)
	}

}

func getLastMsgIdFromDB() (int64, error){
	collection := gDBClient.Database(gConfig.MongoDBName).Collection("LastMsgId")
	var data common.DBLastMsgId
	filter := bson.D{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	result := collection.FindOne(ctx, filter)
	cancel()
	if result.Err() != nil{
		rlog.Printf("[%+v]", result.Err())
		return 0, result.Err()
	}
	err := result.Decode(&data)
	if err != nil {
		rlog.Printf("[%+v]", err)
		return 0, err
	}

	return data.LastMsgId, nil
}

//gRpc proto///////////////////////////////////////////
type serverT struct{}

func (s *serverT) GenMsgId(ctx context.Context, in *pb.GenMsgIdReq) (*pb.GenMsgIdRsp, error) {
	gMsgIdGen.Lock()
	defer gMsgIdGen.Unlock()

	gMsgIdGen.lastMsgId++

	collection := gDBClient.Database(gConfig.MongoDBName).Collection("LastMsgId")
	filter := bson.D{}
	data := bson.D{{"LastMsgId", gMsgIdGen.lastMsgId}}
	_, err := collection.UpdateOne(ctx, filter, bson.D{{"$set", data}})
	if err != nil{
		rlog.Printf("GenMsgId update failed. err=[%+v]", err)
		return nil, err
	}

	return &pb.GenMsgIdRsp{MsgId:gMsgIdGen.lastMsgId}, nil
}
//gRpc proto end///////////////////////////////////////////

