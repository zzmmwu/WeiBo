/*
@Author : Ryan.wuxiaoyong
*/

package main


import (
	"WeiBo/common"
	"WeiBo/common/dbConnPool"
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync/atomic"

	//"io"

	//"WeiBo/common"
	"context"
	"encoding/json"
	//"google.golang.org/grpc/keepalive"
	"io/ioutil"
	//"log"
	"net"
	//"sync"
	"time"
	"fmt"
	"google.golang.org/grpc"
	"os"
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
	MongoDBUrl string `json: "mongoDBUrl"`
	MongoDBName string `json: "mongoDBName"`
	FollowColCount int `json: "followColCount"`
	UserLevelColCount int `json: "userLevelColCount"`
	FollowedColCount int `json: "followedColCount"`
}
//配置文件数据对象
var gConfig = &configType{}

//db连接池
var gDBConnPool = dbConnPool.MongoConnPool{}

//req的计数器，为线程安全请用atomic.AddUint64()进行操作
var gReqFollowCounter int64
var gReqUnFollowCounter int64
//rsp计数器，为线程安全请用atomic.AddUint64()进行操作
var gRspFollowCounter int64
var gRspUnFollowCounter int64


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
	err = gDBConnPool.Init(200, gConfig.MongoDBUrl, "", "")
	if err != nil{
		rlog.Fatalf("%+v", err)
	}
	rlog.Printf("dbpool init successed")

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
		}
		rlog.StatPoint("totalReport", dataArr)
	}
}

//gRpc proto///////////////////////////////////////////
type serverT struct{}

func (s *serverT) Follow(ctx context.Context, in *pb.FollowReq) (*pb.FollowRsp, error) {
	//统计计数
	atomic.AddInt64(&gReqFollowCounter, 1)

	//rlog.Printf("Follow. in=[%+v]", in)
	//DB
	{
		client, err := gDBConnPool.WaitForOneConn(ctx)
		if err != nil{
			rlog.Printf("gDBConnPool.WaitForOneConn failed. err=[%+v]", err)
			return nil, err
		}
		defer gDBConnPool.ReturnConn(client)

		//先改followed表
		{
			//先查询followedColName
			followedColName := ""
			{
				filter := bson.D{{"userid", in.FollowedUserId}}
				colName := "UserLevel_" + fmt.Sprintf("%d", in.FollowedUserId%int64(gConfig.UserLevelColCount))
				//rlog.Printf("colName=%s", colName)
				levelCol := client.Database(gConfig.MongoDBName).Collection(colName)
				result := levelCol.FindOne(ctx, filter)

				var levelData common.DBUserLevel

				if result.Err() == mongo.ErrNoDocuments {
					//按level 0
					followedColName = "Followed_" + fmt.Sprintf("0_%d", in.FollowedUserId%int64(gConfig.FollowedColCount))
					//rlog.Printf("ErrNoDocuments. followedColName=%s", followedColName)
				} else {
					if result.Err() != nil {
						rlog.Printf("[%+v]", result.Err())
						return nil, result.Err()
					}

					err = result.Decode(&levelData)
					if err != nil {
						rlog.Printf("[%+v]", err)
						return nil, err
					}

					//
					if levelData.Level <common.UserLevelSuper {
						followedColName = "Followed_" + fmt.Sprintf("%d_%d", levelData.Level, in.FollowedUserId%int64(gConfig.FollowedColCount))
					} else {
						//超V，单独一张表
						followedColName = common.FollowedColNameOfVIP(in.FollowedUserId)
					}
				}
				if !levelData.InTrans {
					//粉丝数加一
					filter := bson.D{{"userid", in.FollowedUserId}}
					levelData.FollowerCount++
					upsert := true
					_, err := levelCol.UpdateOne(ctx, filter, bson.D{
						{
							"$inc", bson.D{{"followercount", 1}},
						},
						{
							"$setOnInsert", bson.D{{"level", 0}, {"intrans", false}, {"transbegintime", 0}},
						},
					}, &options.UpdateOptions{Upsert: &upsert})
					if err != nil {
						rlog.Printf("followed update failed. err=[%+v]", err)
						return nil, err
					}
					//rlog.Printf("updatereault=[%+v]", result)

				} else {
					//该user正在数据转移，插入临时表
					followedColName = "TransFollowed"
				}
			}

			//rlog.Printf("followedColName=%s", followedColName)

			//插入
			{
				filter := bson.D{{"userid", in.FollowedUserId}, {"followedid", in.UserId}}
				data := bson.D{{"userid", in.FollowedUserId}, {"followedid", in.UserId}, {"followtime", time.Now().Unix()}}
				collection := client.Database(gConfig.MongoDBName).Collection(followedColName)
				upsert := true
				_, err := collection.UpdateOne(ctx, filter, bson.D{{"$set", data}}, &options.UpdateOptions{Upsert:&upsert})
				if err != nil{
					rlog.Printf("followed update failed. err=[%+v]", err)
					return nil, err
				}
				//rlog.Printf("updatereault=[%+v]", result)
			}
		}

		//再改follow表
		{
			//
			colName := fmt.Sprintf("Follow_%d", in.UserId%int64(gConfig.FollowColCount))
			filter := bson.D{{"userid", in.UserId}, {"followid", in.FollowedUserId}}
			data := bson.D{{"userid", in.UserId}, {"followid", in.FollowedUserId}, {"followtime", time.Now().Unix()}}
			collection := client.Database(gConfig.MongoDBName).Collection(colName)
			upsert := true
			_, err := collection.UpdateOne(ctx, filter, bson.D{{"$set", data}}, &options.UpdateOptions{Upsert:&upsert})
			if err != nil{
				rlog.Printf("follow update failed. err=[%+v]", err)
				return nil, err
			}
		}
	}

	//
	err := callFollowedSvr(ctx, true, in.UserId, in.FollowedUserId, &gFollowedSvrConn)
	if err != nil{
		rlog.Printf("callFollowedSvr failed. userid=%d followedId=%d. err=[%+v]", in.UserId, in.FollowedUserId, err)
		return nil, err
	}
	//
	err = callFollowSvr(ctx, true, in.UserId, in.FollowedUserId, &gFollowSvrConn)
	if err != nil{
		rlog.Printf("callFollowSvr failed. userid=%d followedId=%d. err=[%+v]", in.UserId, in.FollowedUserId, err)
		return nil, err
	}

	//统计计数
	atomic.AddInt64(&gRspFollowCounter, 1)
	return &pb.FollowRsp{}, nil
}
func (s *serverT) UnFollow(ctx context.Context, in *pb.UnFollowReq) (*pb.UnFollowRsp, error) {
	//统计计数
	atomic.AddInt64(&gReqUnFollowCounter, 1)

	//DB
	{
		client, err := gDBConnPool.WaitForOneConn(ctx)
		if err != nil{
			rlog.Printf("gDBConnPool.WaitForOneConn failed. err=[%+v]", err)
			return nil, err
		}
		defer gDBConnPool.ReturnConn(client)

		//先改follow表
		{
			//
			colName := fmt.Sprintf("Follow_%d", in.UserId%int64(gConfig.FollowColCount))
			filter := bson.D{{"userid", in.UserId}, {"followid", in.UnFollowedUserId}}
			collection := client.Database(gConfig.MongoDBName).Collection(colName)
			_, err := collection.DeleteMany(ctx, filter)
			if err != nil{
				rlog.Printf("follow delete failed. err=[%+v]", err)
				return nil, err
			}
		}

		//再改followed表
		{
			//先查询followedColName
			followedColName := ""
			inTrans := false
			{
				filter := bson.D{{"userid", in.UnFollowedUserId}}
				colName := "UserLevel_" + fmt.Sprintf("%d", in.UnFollowedUserId%int64(gConfig.UserLevelColCount))
				//rlog.Printf("colName=%s", colName)
				levelCol := client.Database(gConfig.MongoDBName).Collection(colName)
				result := levelCol.FindOne(ctx, filter)

				var levelData common.DBUserLevel

				if result.Err() == mongo.ErrNoDocuments {
					//按level 0
					followedColName = "Followed_" + fmt.Sprintf("0_%d", in.UnFollowedUserId%int64(gConfig.FollowedColCount))
					//rlog.Printf("ErrNoDocuments. followedColName=%s", followedColName)
				} else {
					if result.Err() != nil {
						rlog.Printf("[%+v]", result.Err())
						return nil, result.Err()
					}

					err = result.Decode(&levelData)
					if err != nil {
						rlog.Printf("[%+v]", err)
						return nil, err
					}

					//
					if levelData.Level <= 2 {
						followedColName = "Followed_" + fmt.Sprintf("%d_%d", levelData.Level, in.UnFollowedUserId%int64(gConfig.FollowedColCount))
					} else {
						//大V，单独一张表
						followedColName = common.FollowedColNameOfVIP(in.UnFollowedUserId)
					}
				}
				if !levelData.InTrans {
					inTrans = false

					//粉丝数减一
					filter := bson.D{{"userid", in.UnFollowedUserId}}
					levelData.FollowerCount--
					if levelData.FollowerCount < 0{
						levelData.FollowerCount = 0
					}
					_, err := levelCol.UpdateOne(ctx, filter, bson.D{
						{
							"$inc", bson.D{{"followercount", -1}}, //需要防止减成负数
						},
					})
					if err != nil {
						rlog.Printf("followed update failed. err=[%+v]", err)
						return nil, err
					}
					//rlog.Printf("updatereault=[%+v]", result)
				} else {
					inTrans = true

					//该user正在数据转移，插入临时表
					followedColName = "TransUnFollowed"
					//插入
					{
						filter := bson.D{{"userid", in.UnFollowedUserId}, {"followedid", in.UserId}}
						data := bson.D{{"userid", in.UnFollowedUserId}, {"followedid", in.UserId}}
						collection := client.Database(gConfig.MongoDBName).Collection(followedColName)
						upsert := true
						_, err := collection.UpdateOne(ctx, filter, bson.D{{"$set", data}}, &options.UpdateOptions{Upsert:&upsert})
						if err != nil{
							rlog.Printf("followed update failed. err=[%+v]", err)
							return nil, err
						}
						//rlog.Printf("updatereault=[%+v]", result)
					}
				}
			}


			//正常删除
			if ! inTrans{
				filter := bson.D{{"userid", in.UnFollowedUserId}, {"followedid", in.UserId}}
				collection := client.Database(gConfig.MongoDBName).Collection(followedColName)
				_, err := collection.DeleteMany(ctx, filter)
				if err != nil{
					rlog.Printf("followed delete failed. err=[%+v]", err)
					return nil, err
				}
			}
		}
	}

	//
	err := callFollowedSvr(ctx, false, in.UserId, in.UnFollowedUserId, &gFollowedSvrConn)
	if err != nil{
		rlog.Printf("callFollowedSvr failed. userid=%d followedId=%d. err=[%+v]", in.UserId, in.UnFollowedUserId, err)
		return nil, err
	}
	//
	err = callFollowSvr(ctx, false, in.UserId, in.UnFollowedUserId, &gFollowSvrConn)
	if err != nil{
		rlog.Printf("callFollowSvr failed. userid=%d followedId=%d. err=[%+v]", in.UserId, in.UnFollowedUserId, err)
		return nil, err
	}

	//统计计数
	atomic.AddInt64(&gRspUnFollowCounter, 1)
	return &pb.UnFollowRsp{}, nil
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



