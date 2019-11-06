/*
@Author : Ryan.wuxiaoyong
*/

package main


import (
	"WeiBo/common"
	//"WeiBo/common/dbConnPool"
	"WeiBo/common/grpcStatsHandler"
	"context"
	"encoding/json"
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io/ioutil"
	//"sync/atomic"

	//"log"
	"net"
	"sync"
	"time"
	"fmt"

	//"errors"
	"google.golang.org/grpc"
	"os"

	pb "WeiBo/common/protobuf"
	"WeiBo/common/rlog"
)

//配置文件格式
type configType struct {
	MyName string 				`json: "myName"`
	RlogSvrAddr string			`json: "rlogSvrAddr"`
	ListenAddr string			`json: "listenAddr"`
	DBName string				`json: "dbName"`
	UserLevelColCount int		`json: "userLevelColCount"`
	FollowedColCount int		`json: "followedColCount"`
	FollowColCount int			`json: "followColCount"`
	UserMsgIdColCount int		`json: "userMsgIdColCount"`
	ContentColCount int			`json: "contentColCount"`

	DBCluster []dbConfigT		`json: "dbCluster"`
}
type dbConfigT struct{
	Url string					`json: "url"`
	Tag string					`json: "tag"`
	ConnNum int					`json: "connNum"`
	ColRangeArr []colRangeT		`json: "colRangeArr"`
}
type colRangeT struct {
	ColType string				`json: "colType"`
	StartIndex int				`json: "startIndex"`
	EndIndex int				`json: "endIndex"`
}
//配置文件数据对象
var gConfig = &configType{}

//客户端连接统计
var gConnCounterHandle = grpcStatsHandler.ConnCounterHandler{}

const (
	dbCmdQueryFollow = 1
	//dbCmdQueryUserLevel = 2
	dbCmdQueryFollower = 3
	dbCmdQueryUserMsgId = 4
	dbCmdQueryMsgContent = 5
	dbCmdInsertFollow = 6
	dbCmdInsertFollower = 7
	dbCmdDelFollow = 8
	dbCmdDelFollower = 9
	dbCmdInsertUserMsgId = 10
	dbCmdInsertMsgContent = 11
)
type dbReqCmdT struct {
	reqId int64

	cmd int
	req interface{}
}
type dbRspCmdT struct {
	reqId int64

	rsp interface{}
}
//
type dbClusterDataT struct{
	dbArr []*dbDataT
}
type dbDataT struct {
	url string
	tag string
	reqChn chan *dbReqCmdT
}
//
var gDBClusterData = dbClusterDataT{dbArr:[]*dbDataT{}}
//集合名到db 的映射
var gColName2DBReqChnMap = make(map[string]chan *dbReqCmdT)
//目前Followed不支持分DB，所有UserLevel表和Followed表都只能在一台DB上。post操作的量并不大，后续有需要再实现分DB
var gFollowDBReqChn chan *dbReqCmdT

//接受rsp的管道map，reqId为key
type rspChanMapT struct {
	sync.RWMutex

	reqId2ChnMap map[int64]chan *dbRspCmdT
}
func (m *rspChanMapT)getChn(reqId int64) (chan *dbRspCmdT, bool) {
	m.RLock()
	defer m.RUnlock()
	rsp, ok := m.reqId2ChnMap[reqId]
	return rsp, ok
}
func (m *rspChanMapT)insertChn(reqId int64, chn chan *dbRspCmdT) {
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
var gRspChanMap = rspChanMapT{sync.RWMutex{}, map[int64]chan *dbRspCmdT{}}


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

	//每一个db一条独立的reqChan
	url2chnMap := make(map[string]chan *dbReqCmdT)
	for _, db := range gConfig.DBCluster{
		chn := make(chan *dbReqCmdT)
		url2chnMap[db.Url] = chn

		gDBClusterData.dbArr = append(gDBClusterData.dbArr, &dbDataT{url: db.Url, tag: db.Tag, reqChn: chn})
	}

	//集合名到reqChan的映射
	makeColName2DBMap(url2chnMap)

	//db处理toutine
	for _, db := range gConfig.DBCluster{
		for i:=0; i<db.ConnNum; i++{
			go dbProcRoutine(db.Url, "", "", url2chnMap[db.Url])
		}
	}

	//开启数据打点routine
	go statReportRoutine()

	//开启grpc服务
	lis, err := net.Listen("tcp", gConfig.ListenAddr)
	if err != nil {
		rlog.Fatalf("failed to listen: %+v", err)
	}
	rlog.Printf("begin to Listen [%s]", gConfig.ListenAddr)
	svrOpt := grpc.MaxSendMsgSize(40*1024*1024) //40M
	handler := grpc.StatsHandler(&gConnCounterHandle)
	grpcServer := grpc.NewServer(svrOpt, handler)
	pb.RegisterDbSvrServer(grpcServer, &serverT{})
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

			for _, db := range gDBClusterData.dbArr{
				data = rlog.StatPointData{Name:fmt.Sprintf("[%s]chnLen", db.tag), Data:int64(len(db.reqChn))}
				dataArr = append(dataArr, data)
			}
			data = rlog.StatPointData{Name:"[Followed]chnLen", Data:int64(len(gFollowDBReqChn))}
			dataArr = append(dataArr, data)
		}
		rlog.StatPoint("totalReport", dataArr)
	}
}

func makeColName2DBMap(url2chnMap map[string]chan *dbReqCmdT){
	//msgContent
	for i:=0; i<gConfig.ContentColCount; i++{
		found := false
		for _, db := range gConfig.DBCluster {
			for _, colData := range db.ColRangeArr{
				if colData.ColType == "MsgContent"{
					if colData.StartIndex < 0{
						found = true
					}else if colData.StartIndex <= i && colData.EndIndex >= i{
						found = true
					}
					if found{
						colName := fmt.Sprintf("MsgContent_%d", i)
						gColName2DBReqChnMap[colName] = url2chnMap[db.Url]
						break
					}
				}
			}
			if found{
				break
			}
		}
		if !found{
			rlog.Fatalf("MsgContent_%d has no db", i)
		}
	}

	//UserMsgId
	for i:=0; i<gConfig.UserMsgIdColCount; i++{
		found := false
		for _, db := range gConfig.DBCluster {
			for _, colData := range db.ColRangeArr{
				if colData.ColType == "UserMsgId"{
					if colData.StartIndex < 0{
						found = true
					}else if colData.StartIndex <= i && colData.EndIndex >= i{
						found = true
					}
					if found{
						colName := fmt.Sprintf("UserMsgId_%d", i)
						gColName2DBReqChnMap[colName] = url2chnMap[db.Url]
						break
					}
				}
			}
			if found{
				break
			}
		}
		if !found{
			rlog.Fatalf("UserMsgId_%d has no db", i)
		}
	}

	//Follow
	for i:=0; i<gConfig.FollowColCount; i++{
		found := false
		for _, db := range gConfig.DBCluster {
			for _, colData := range db.ColRangeArr{
				if colData.ColType == "Follow"{
					if colData.StartIndex < 0{
						found = true
					}else if colData.StartIndex <= i && colData.EndIndex >= i{
						found = true
					}
					if found{
						colName := fmt.Sprintf("Follow_%d", i)
						gColName2DBReqChnMap[colName] = url2chnMap[db.Url]
						break
					}
				}
			}
			if found{
				break
			}
		}
		if !found{
			rlog.Fatalf("Follow_%d has no db", i)
		}
	}

	//Followed
	{
		found := false
		for _, db := range gConfig.DBCluster {
			for _, colData := range db.ColRangeArr{
				if colData.ColType == "Followed"{
					found = true
					gFollowDBReqChn = url2chnMap[db.Url]
					break
				}
			}
			if found{
				break
			}
		}
		if !found{
			rlog.Fatalf("Followed has no db")
		}
	}
}


func dbProcRoutine(mongoUrl string, usrName string, pass string, reqChn chan *dbReqCmdT){
	//
	mongoOpt := options.Client().ApplyURI("mongodb://" + mongoUrl)
	if usrName != "" {
		mongoOpt = mongoOpt.SetAuth(options.Credential{Username:usrName, Password:pass})
	}
	//超时设置
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	client, err := mongo.Connect(ctx, mongoOpt)
	cancel()
	if err != nil {
		rlog.Fatalf("mongodb connect failed. [%+v]", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	err = client.Ping(ctx, nil)
	cancel()
	if err != nil {
		rlog.Fatalf("mongodb ping failed. [%+v]", err)
	}

	for {
		reqCmd := <-reqChn

		rspCmd := dbRspCmdT{reqId:reqCmd.reqId}
		switch reqCmd.cmd {
		case dbCmdQueryFollow:
			rsp, err := queryFollowProc(client, reqCmd.req.(*pb.DBQueryFollowReq))
			if err != nil{
				//丢弃
				continue
			}
			rspCmd.rsp = rsp
			break
		case dbCmdQueryFollower:
			rsp, err := queryFollowerProc(client, reqCmd.req.(*pb.DBQueryFollowerReq))
			if err != nil{
				//丢弃
				continue
			}
			rspCmd.rsp = rsp
			break
		case dbCmdQueryMsgContent:
			rsp, err := queryMsgContentProc(client, reqCmd.req.(*pb.DBQueryMsgContentReq))
			if err != nil{
				//丢弃
				continue
			}
			rspCmd.rsp = rsp
			break
		case dbCmdQueryUserMsgId:
			rsp, err := queryUserMsgIdProc(client, reqCmd.req.(*pb.DBQueryUserMsgIdReq))
			if err != nil{
				//丢弃
				continue
			}
			rspCmd.rsp = rsp
			break
		case dbCmdInsertFollow:
			rsp, err := insertFollowProc(client, reqCmd.req.(*pb.DBFollowReq))
			if err != nil{
				//丢弃
				continue
			}
			rspCmd.rsp = rsp
			break
		case dbCmdInsertFollower:
			rsp, err := insertFollowerProc(client, reqCmd.req.(*pb.DBFollowReq))
			if err != nil{
				//丢弃
				continue
			}
			rspCmd.rsp = rsp
			break
		case dbCmdInsertUserMsgId:
			rsp, err := insertUserMsgIdProc(client, reqCmd.req.(*pb.DBPostReq))
			if err != nil{
				//丢弃
				continue
			}
			rspCmd.rsp = rsp
			break
		case dbCmdInsertMsgContent:
			rsp, err := insertMsgContentProc(client, reqCmd.req.(*pb.DBPostReq))
			if err != nil{
				//丢弃
				continue
			}
			rspCmd.rsp = rsp
			break
		case dbCmdDelFollow:
			rsp, err := delFollowProc(client, reqCmd.req.(*pb.DBUnFollowReq))
			if err != nil{
				//丢弃
				continue
			}
			rspCmd.rsp = rsp
			break
		case dbCmdDelFollower:
			rsp, err := delFollowerProc(client, reqCmd.req.(*pb.DBUnFollowReq))
			if err != nil{
				//丢弃
				continue
			}
			rspCmd.rsp = rsp
			break
		}

		rspChn, ok := gRspChanMap.getChn(reqCmd.reqId)
		if ok {
			rspChn<- &rspCmd
		}else{
			//超时了，扔掉
		}
	}
}

func queryFollowProc(client *mongo.Client, req *pb.DBQueryFollowReq) (*pb.DBQueryFollowRsp, error){
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	//分表后的colName
	colName := fmt.Sprintf("Follow_%d", req.UserId%int64(gConfig.FollowColCount))
	collection := client.Database(gConfig.DBName).Collection(colName)
	//rlog.Printf("colName:%+s userId:%d", colName, userId)

	var data common.DBFollow
	filter := bson.D{{"userid", req.UserId}}
	cursor, err := collection.Find(ctx, filter)
	if err != nil{
		rlog.Printf("find userid=[%d] failed. err=[%+v]", req.UserId, err)
		return &pb.DBQueryFollowRsp{}, err
	}
	if cursor.Err() != nil{
		rlog.Printf("find userid=[%d] failed. err=[%+v]", req.UserId, cursor.Err())
		return &pb.DBQueryFollowRsp{}, err
	}
	//
	rsp := pb.DBQueryFollowRsp{FollowIdArr:[]int64{}}
	for cursor.Next(ctx){
		err = cursor.Decode(&data)
		if err != nil {
			rlog.Printf("[%+v]", err)
			return &pb.DBQueryFollowRsp{}, err
		}
		//rlog.Printf("cursor:[%+v", cursor)
		//rlog.Printf("DBFollow data:[%+v", data)
		rsp.FollowIdArr = append(rsp.FollowIdArr, data.FollowId)
	}

	return &rsp, nil
}
func queryFollowerProc(client *mongo.Client, req *pb.DBQueryFollowerReq)  (*pb.DBQueryFollowerRsp, error){
	userId := req.UserId

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	///先查询followedColName
	followedColName := ""
	userLevel := common.UserLevelNormal
	{
		filter := bson.D{{"userid", userId}}
		colName := "UserLevel_" + fmt.Sprintf("%d", userId%int64(gConfig.UserLevelColCount))
		collection := client.Database(gConfig.DBName).Collection(colName)
		result := collection.FindOne(ctx, filter)
		if result.Err() == mongo.ErrNoDocuments {
			//按level 0
			followedColName = "Followed_" + fmt.Sprintf("0_%d", userId%int64(gConfig.FollowedColCount))
		}else{
			if result.Err() != mongo.ErrNoDocuments && result.Err() != nil {
				rlog.Printf("[%+v]", result.Err())
				return &pb.DBQueryFollowerRsp{}, result.Err()
			}
			var data common.DBUserLevel
			err := result.Decode(&data)
			if err != nil {
				rlog.Printf("[%+v]", err)
				return &pb.DBQueryFollowerRsp{}, err
			}

			//
			if data.Level < common.UserLevelSuper{
				followedColName = "Followed_" + fmt.Sprintf("%d_%d", data.Level, userId%int64(gConfig.FollowedColCount))
			}else{
				//大V，单独一张表
				followedColName = common.FollowedColNameOfVIP(userId)
				userLevel = common.UserLevelSuper
			}
		}
	}

	//
	collection := client.Database(gConfig.DBName).Collection(followedColName)
	var data common.DBFollowed
	filter := bson.D{{"userid", userId}}
	if userLevel == common.UserLevelSuper{
		filter = bson.D{}
	}
	cursor, err := collection.Find(ctx, filter)
	if err != nil{
		rlog.Printf("find userid=[%d] failed. err=[%+v]", userId, err)
		return &pb.DBQueryFollowerRsp{}, err
	}
	if cursor.Err() != nil{
		rlog.Printf("find userid=[%d] failed. err=[%+v]", userId, cursor.Err())
		return &pb.DBQueryFollowerRsp{}, err
	}
	//
	rsp := pb.DBQueryFollowerRsp{FollowerIdArr:[]int64{}}
	for cursor.Next(ctx){
		err = cursor.Decode(&data)
		if err != nil {
			rlog.Printf("[%+v]", err)
			return &pb.DBQueryFollowerRsp{}, err
		}
		rsp.FollowerIdArr = append(rsp.FollowerIdArr, data.FollowedId)
	}

	return &rsp, nil
}
func queryUserMsgIdProc(client *mongo.Client, req *pb.DBQueryUserMsgIdReq)  (*pb.DBQueryUserMsgIdRsp, error){
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	userId := req.UserId
	//分表后的colName
	colName := fmt.Sprintf("UserMsgId_%d", userId%int64(gConfig.UserMsgIdColCount))
	collection := client.Database(gConfig.DBName).Collection(colName)

	var userMsgId common.DBUserMsgId
	filter := bson.D{{"userid", userId}}
	cursor, err := collection.Find(ctx, filter)
	if err != nil{
		rlog.Printf("find userid=[%d] failed. err=[%+v]", userId, err)
		return &pb.DBQueryUserMsgIdRsp{}, err
	}
	if cursor.Err() != nil{
		rlog.Printf("find userid=[%d] failed. err=[%+v]", userId, cursor.Err())
		return &pb.DBQueryUserMsgIdRsp{}, err
	}
	//
	rsp := pb.DBQueryUserMsgIdRsp{MsgIdArr:[]int64{}}
	for cursor.Next(ctx){
		err = cursor.Decode(&userMsgId)
		if err != nil {
			rlog.Printf("[%+v]", err)
			return &pb.DBQueryUserMsgIdRsp{}, err
		}
		rsp.MsgIdArr = append(rsp.MsgIdArr, userMsgId.MsgId)
	}

	return &rsp, nil
}
func queryMsgContentProc(client *mongo.Client, req *pb.DBQueryMsgContentReq)  (*pb.DBQueryMsgContentRsp, error){
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msgId := req.MsgId
	//分表后的colName
	colName := fmt.Sprintf("MsgContent_%d", msgId%int64(gConfig.ContentColCount))
	collection := client.Database(gConfig.DBName).Collection(colName)
	//log.Printf("find MsgId=%d, colName=%s", msgId, colName)

	var content common.DBMsgContent
	filter := bson.D{{"msgid", msgId}}
	result := collection.FindOne(ctx, filter)
	if result.Err() != nil{
		rlog.Printf("[%+v]", result.Err())
		return &pb.DBQueryMsgContentRsp{}, result.Err()
	}
	err := result.Decode(&content)
	if err != nil {
		rlog.Printf("[%+v]", err)
		return &pb.DBQueryMsgContentRsp{}, err
	}

	return &pb.DBQueryMsgContentRsp{Msg: &pb.MsgData{MsgId:content.MsgId, Text:content.Text, VideoUrl:content.VideoUrl, ImgUrlArr:content.ImgUrlArr}}, nil
}
func insertFollowProc(client *mongo.Client, req *pb.DBFollowReq)  (*pb.DBFollowRsp, error){
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	userId := req.UserId
	followId := req.FollowId
	colName := fmt.Sprintf("Follow_%d", userId%int64(gConfig.FollowColCount))
	filter := bson.D{{"userid", userId}, {"followid", followId}}
	data := bson.D{{"userid", userId}, {"followid", followId}, {"followtime", time.Now().Unix()}}
	collection := client.Database(gConfig.DBName).Collection(colName)
	upsert := true
	_, err := collection.UpdateOne(ctx, filter, bson.D{{"$set", data}}, &options.UpdateOptions{Upsert:&upsert})
	if err != nil{
		rlog.Printf("follow update failed. err=[%+v]", err)
		return &pb.DBFollowRsp{}, err
	}

	return &pb.DBFollowRsp{}, nil
}
func insertFollowerProc(client *mongo.Client, req *pb.DBFollowReq)  (*pb.DBFollowRsp, error){
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	userId := req.UserId
	followId := req.FollowId

	//先查询followedColName
	followedColName := ""
	{
		filter := bson.D{{"userid", followId}}
		colName := "UserLevel_" + fmt.Sprintf("%d", followId%int64(gConfig.UserLevelColCount))
		//rlog.Printf("colName=%s", colName)
		levelCol := client.Database(gConfig.DBName).Collection(colName)
		result := levelCol.FindOne(ctx, filter)

		var levelData common.DBUserLevel

		if result.Err() == mongo.ErrNoDocuments {
			//按level 0
			followedColName = "Followed_" + fmt.Sprintf("0_%d", followId%int64(gConfig.FollowedColCount))
			//rlog.Printf("ErrNoDocuments. followedColName=%s", followedColName)
		} else {
			if result.Err() != nil {
				rlog.Printf("[%+v]", result.Err())
				return &pb.DBFollowRsp{}, result.Err()
			}

			err := result.Decode(&levelData)
			if err != nil {
				rlog.Printf("[%+v]", err)
				return &pb.DBFollowRsp{}, err
			}

			//
			if levelData.Level <common.UserLevelSuper {
				followedColName = "Followed_" + fmt.Sprintf("%d_%d", levelData.Level, followId%int64(gConfig.FollowedColCount))
			} else {
				//超V，单独一张表
				followedColName = common.FollowedColNameOfVIP(followId)
			}
		}
		if !levelData.InTrans {
			//粉丝数加一
			filter := bson.D{{"userid", followId}}
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
				return &pb.DBFollowRsp{}, err
			}
			//rlog.Printf("updatereault=[%+v]", result)

		} else {
			//该user正在数据转移，插入临时表
			followedColName = "TransFollowed"
		}
	}


	//插入
	{
		filter := bson.D{{"userid", followId}, {"followedid", userId}}
		data := bson.D{{"userid", followId}, {"followedid", userId}, {"followtime", time.Now().Unix()}}
		collection := client.Database(gConfig.DBName).Collection(followedColName)
		upsert := true
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := collection.UpdateOne(ctx, filter, bson.D{{"$set", data}}, &options.UpdateOptions{Upsert:&upsert})
		cancel()
		if err != nil{
			rlog.Printf("followed update failed. err=[%+v]", err)
			return &pb.DBFollowRsp{}, err
		}
		//rlog.Printf("updatereault=[%+v]", result)
	}

	return &pb.DBFollowRsp{}, nil
}
func insertMsgContentProc(client *mongo.Client, req *pb.DBPostReq)  (*pb.DBPostRsp, error){
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data := common.DBMsgContent{MsgId:req.Content.MsgId, Text:req.Content.Text, VideoUrl:req.Content.VideoUrl, ImgUrlArr:req.Content.ImgUrlArr}
	colName := "MsgContent_"+fmt.Sprintf("%d", req.Content.MsgId%int64(gConfig.ContentColCount))
	collection := client.Database(gConfig.DBName).Collection(colName)
	_, err := collection.InsertOne(ctx, data)
	if err != nil {
		rlog.Printf("insert into %s failed. err=%+v", colName, err)
		return &pb.DBPostRsp{}, err
	}

	return &pb.DBPostRsp{}, nil
}
func insertUserMsgIdProc(client *mongo.Client, req *pb.DBPostReq)  (*pb.DBPostRsp, error){
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data := common.DBUserMsgId{UserId:req.Content.UserId, MsgId:req.Content.MsgId}
	colName := "UserMsgId_"+fmt.Sprintf("%d", req.Content.UserId%int64(gConfig.UserMsgIdColCount))
	collection := client.Database(gConfig.DBName).Collection(colName)
	_, err := collection.InsertOne(ctx, data)
	if err != nil {
		rlog.Printf("insert into %s failed. err=%+v", colName, err)
		return &pb.DBPostRsp{}, err
	}

	return &pb.DBPostRsp{}, nil
}
func delFollowProc(client *mongo.Client, req *pb.DBUnFollowReq)  (*pb.DBUnFollowRsp, error){
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	userId := req.UserId
	unFollowId := req.FollowId

	colName := fmt.Sprintf("Follow_%d", userId%int64(gConfig.FollowColCount))
	filter := bson.D{{"userid", userId}, {"followid", unFollowId}}
	collection := client.Database(gConfig.DBName).Collection(colName)
	_, err := collection.DeleteMany(ctx, filter)
	if err != nil{
		rlog.Printf("follow delete failed. err=[%+v]", err)
		return &pb.DBUnFollowRsp{}, err
	}

	return &pb.DBUnFollowRsp{}, nil
}
func delFollowerProc(client *mongo.Client, req *pb.DBUnFollowReq)  (*pb.DBUnFollowRsp, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	userId := req.UserId
	unFollowId := req.FollowId

	//先查询followedColName
	followedColName := ""
	inTrans := false
	{
		filter := bson.D{{"userid", unFollowId}}
		colName := "UserLevel_" + fmt.Sprintf("%d", unFollowId%int64(gConfig.UserLevelColCount))
		//rlog.Printf("colName=%s", colName)
		levelCol := client.Database(gConfig.DBName).Collection(colName)
		result := levelCol.FindOne(ctx, filter)

		var levelData common.DBUserLevel

		if result.Err() == mongo.ErrNoDocuments {
			//按level 0
			followedColName = "Followed_" + fmt.Sprintf("0_%d", unFollowId%int64(gConfig.FollowedColCount))
			//rlog.Printf("ErrNoDocuments. followedColName=%s", followedColName)
		} else {
			if result.Err() != nil {
				rlog.Printf("[%+v]", result.Err())
				return &pb.DBUnFollowRsp{}, result.Err()
			}

			err := result.Decode(&levelData)
			if err != nil {
				rlog.Printf("[%+v]", err)
				return &pb.DBUnFollowRsp{}, err
			}

			//
			if levelData.Level <= 2 {
				followedColName = "Followed_" + fmt.Sprintf("%d_%d", levelData.Level, unFollowId%int64(gConfig.FollowedColCount))
			} else {
				//大V，单独一张表
				followedColName = common.FollowedColNameOfVIP(unFollowId)
			}
		}
		if !levelData.InTrans {
			inTrans = false

			//粉丝数减一
			filter := bson.D{{"userid", unFollowId}}
			levelData.FollowerCount--
			if levelData.FollowerCount < 0 {
				levelData.FollowerCount = 0
			}
			_, err := levelCol.UpdateOne(ctx, filter, bson.D{
				{
					"$inc", bson.D{{"followercount", -1}}, //需要防止减成负数
				},
			})
			if err != nil {
				rlog.Printf("followed update failed. err=[%+v]", err)
				return &pb.DBUnFollowRsp{}, err
			}
			//rlog.Printf("updatereault=[%+v]", result)
		} else {
			inTrans = true

			//该user正在数据转移，插入临时表
			followedColName = "TransUnFollowed"
			//插入
			{
				filter := bson.D{{"userid", unFollowId}, {"followedid", userId}}
				data := bson.D{{"userid", unFollowId}, {"followedid", userId}}
				collection := client.Database(gConfig.DBName).Collection(followedColName)
				upsert := true
				_, err := collection.UpdateOne(ctx, filter, bson.D{{"$set", data}}, &options.UpdateOptions{Upsert: &upsert})
				if err != nil {
					rlog.Printf("followed update failed. err=[%+v]", err)
					return &pb.DBUnFollowRsp{}, err
				}
				//rlog.Printf("updatereault=[%+v]", result)
			}
		}
	}

	//正常删除
	if ! inTrans {
		filter := bson.D{{"userid", unFollowId}, {"followedid", userId}}
		collection := client.Database(gConfig.DBName).Collection(followedColName)
		_, err := collection.DeleteMany(ctx, filter)
		if err != nil {
			rlog.Printf("followed delete failed. err=[%+v]", err)
			return &pb.DBUnFollowRsp{}, err
		}
	}

	return &pb.DBUnFollowRsp{}, nil
}


//gRpc proto///////////////////////////////////////////
type serverT struct{}


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

func (s *serverT) QueryFollow(ctx context.Context, in *pb.DBQueryFollowReq) (*pb.DBQueryFollowRsp, error) {
	//为本次请求获取一个reqId
	reqId := gReqIdGen.getOneId()

	req := dbReqCmdT{reqId:reqId, cmd:dbCmdQueryFollow, req: in}

	//创建rsp管道
	rspChn := make(chan *dbRspCmdT, 1)
	gRspChanMap.insertChn(reqId, rspChn)
	//rsp后就扔掉
	defer gRspChanMap.delChn(reqId)

	//确定一个输送管道然后发送req
	colName := fmt.Sprintf("Follow_%d", in.UserId%int64(gConfig.FollowColCount))
	reqChn, ok := gColName2DBReqChnMap[colName]
	if !ok{
		rlog.Printf("gColName2DBReqChnMap[%s] not ok", colName)
		return &pb.DBQueryFollowRsp{}, errors.New("error")
	}
	reqChn<-&req

	//等待rsp或超时
	select {
	case <-ctx.Done():
		return &pb.DBQueryFollowRsp{}, errors.New("timeout")
	case rsp, ok := <-rspChn:
		if !ok{
			rlog.Printf("<-rspChn not ok in follow. reqId=%d", reqId)
			return &pb.DBQueryFollowRsp{}, errors.New("error")
		}

		return rsp.rsp.(*pb.DBQueryFollowRsp), nil
	}
}
func (s *serverT) QueryFollower(ctx context.Context, in *pb.DBQueryFollowerReq) (*pb.DBQueryFollowerRsp, error) {
	//为本次请求获取一个reqId
	reqId := gReqIdGen.getOneId()

	req := dbReqCmdT{reqId:reqId, cmd:dbCmdQueryFollower, req: in}

	//创建rsp管道
	rspChn := make(chan *dbRspCmdT, 1)
	gRspChanMap.insertChn(reqId, rspChn)
	//rsp后就扔掉
	defer gRspChanMap.delChn(reqId)

	//确定一个输送管道然后发送req
	gFollowDBReqChn<-&req

	//等待rsp或超时
	select {
	case <-ctx.Done():
		return &pb.DBQueryFollowerRsp{}, errors.New("timeout")
	case rsp, ok := <-rspChn:
		if !ok{
			rlog.Printf("<-rspChn not ok in follow. reqId=%d", reqId)
			return &pb.DBQueryFollowerRsp{}, errors.New("error")
		}

		return rsp.rsp.(*pb.DBQueryFollowerRsp), nil
	}
}
func (s *serverT) QueryUserMsgId(ctx context.Context, in *pb.DBQueryUserMsgIdReq) (*pb.DBQueryUserMsgIdRsp, error) {
	//为本次请求获取一个reqId
	reqId := gReqIdGen.getOneId()

	req := dbReqCmdT{reqId:reqId, cmd:dbCmdQueryUserMsgId, req: in}

	//创建rsp管道
	rspChn := make(chan *dbRspCmdT, 1)
	gRspChanMap.insertChn(reqId, rspChn)
	//rsp后就扔掉
	defer gRspChanMap.delChn(reqId)

	//确定一个输送管道然后发送req
	colName := fmt.Sprintf("UserMsgId_%d", in.UserId%int64(gConfig.UserMsgIdColCount))
	reqChn, ok := gColName2DBReqChnMap[colName]
	if !ok{
		rlog.Printf("gColName2DBReqChnMap[%s] not ok", colName)
		return &pb.DBQueryUserMsgIdRsp{}, errors.New("error")
	}
	reqChn<-&req

	//等待rsp或超时
	select {
	case <-ctx.Done():
		return &pb.DBQueryUserMsgIdRsp{}, errors.New("timeout")
	case rsp, ok := <-rspChn:
		if !ok{
			rlog.Printf("<-rspChn not ok in follow. reqId=%d", reqId)
			return &pb.DBQueryUserMsgIdRsp{}, errors.New("error")
		}

		return rsp.rsp.(*pb.DBQueryUserMsgIdRsp), nil
	}
}
func (s *serverT) QueryMsgContent(ctx context.Context, in *pb.DBQueryMsgContentReq) (*pb.DBQueryMsgContentRsp, error) {
	//为本次请求获取一个reqId
	reqId := gReqIdGen.getOneId()

	req := dbReqCmdT{reqId:reqId, cmd:dbCmdQueryMsgContent, req: in}

	//创建rsp管道
	rspChn := make(chan *dbRspCmdT, 1)
	gRspChanMap.insertChn(reqId, rspChn)
	//rsp后就扔掉
	defer gRspChanMap.delChn(reqId)

	//确定一个输送管道然后发送req
	colName := fmt.Sprintf("MsgContent_%d", in.MsgId%int64(gConfig.ContentColCount))
	reqChn, ok := gColName2DBReqChnMap[colName]
	if !ok{
		rlog.Printf("gColName2DBReqChnMap[%s] not ok", colName)
		return &pb.DBQueryMsgContentRsp{}, errors.New("error")
	}
	reqChn<-&req

	//等待rsp或超时
	select {
	case <-ctx.Done():
		return &pb.DBQueryMsgContentRsp{}, errors.New("timeout")
	case rsp, ok := <-rspChn:
		if !ok{
			rlog.Printf("<-rspChn not ok in follow. reqId=%d", reqId)
			return &pb.DBQueryMsgContentRsp{}, errors.New("error")
		}

		return rsp.rsp.(*pb.DBQueryMsgContentRsp), nil
	}
}
func (s *serverT) Follow(ctx context.Context, in *pb.DBFollowReq) (*pb.DBFollowRsp, error) {
	//先写followed表
	{
		//为本次请求获取一个reqId
		reqId := gReqIdGen.getOneId()

		req := dbReqCmdT{reqId:reqId, cmd:dbCmdInsertFollower, req: in}

		//创建rsp管道
		rspChn := make(chan *dbRspCmdT, 1)
		gRspChanMap.insertChn(reqId, rspChn)
		//rsp后就扔掉
		defer gRspChanMap.delChn(reqId)

		//确定一个输送管道然后发送req
		gFollowDBReqChn<-&req

		//等待rsp或超时
		select {
		case <-ctx.Done():
			return &pb.DBFollowRsp{}, errors.New("timeout")
		case _, ok := <-rspChn:
			if !ok{
				rlog.Printf("<-rspChn not ok in follow. reqId=%d", reqId)
				return &pb.DBFollowRsp{}, errors.New("error")
			}
		}
	}
	//再写follow表
	{
		//为本次请求获取一个reqId
		reqId := gReqIdGen.getOneId()

		req := dbReqCmdT{reqId:reqId, cmd:dbCmdInsertFollow, req: in}

		//创建rsp管道
		rspChn := make(chan *dbRspCmdT, 1)
		gRspChanMap.insertChn(reqId, rspChn)
		//rsp后就扔掉
		defer gRspChanMap.delChn(reqId)

		//确定一个输送管道然后发送req
		colName := fmt.Sprintf("Follow_%d", in.UserId%int64(gConfig.FollowColCount))
		reqChn, ok := gColName2DBReqChnMap[colName]
		if !ok{
			rlog.Printf("gColName2DBReqChnMap[%s] not ok", colName)
			return &pb.DBFollowRsp{}, errors.New("error")
		}
		reqChn<-&req

		//等待rsp或超时
		select {
		case <-ctx.Done():
			return &pb.DBFollowRsp{}, errors.New("timeout")
		case _, ok := <-rspChn:
			if !ok{
				rlog.Printf("<-rspChn not ok in follow. reqId=%d", reqId)
				return &pb.DBFollowRsp{}, errors.New("error")
			}
		}
	}

	return &pb.DBFollowRsp{}, nil
}
func (s *serverT) UnFollow(ctx context.Context, in *pb.DBUnFollowReq) (*pb.DBUnFollowRsp, error) {
	//先写followed表
	{
		//为本次请求获取一个reqId
		reqId := gReqIdGen.getOneId()

		req := dbReqCmdT{reqId:reqId, cmd:dbCmdDelFollower, req: in}

		//创建rsp管道
		rspChn := make(chan *dbRspCmdT, 1)
		gRspChanMap.insertChn(reqId, rspChn)
		//rsp后就扔掉
		defer gRspChanMap.delChn(reqId)

		//确定一个输送管道然后发送req
		gFollowDBReqChn<-&req

		//等待rsp或超时
		select {
		case <-ctx.Done():
			return &pb.DBUnFollowRsp{}, errors.New("timeout")
		case _, ok := <-rspChn:
			if !ok{
				rlog.Printf("<-rspChn not ok in follow. reqId=%d", reqId)
				return &pb.DBUnFollowRsp{}, errors.New("error")
			}
		}
	}
	//再写follow表
	{
		//为本次请求获取一个reqId
		reqId := gReqIdGen.getOneId()

		req := dbReqCmdT{reqId:reqId, cmd:dbCmdDelFollow, req: in}

		//创建rsp管道
		rspChn := make(chan *dbRspCmdT, 1)
		gRspChanMap.insertChn(reqId, rspChn)
		//rsp后就扔掉
		defer gRspChanMap.delChn(reqId)

		//确定一个输送管道然后发送req
		colName := fmt.Sprintf("Follow_%d", in.UserId%int64(gConfig.FollowColCount))
		reqChn, ok := gColName2DBReqChnMap[colName]
		if !ok{
			rlog.Printf("gColName2DBReqChnMap[%s] not ok", colName)
			return &pb.DBUnFollowRsp{}, errors.New("error")
		}
		reqChn<-&req

		//等待rsp或超时
		select {
		case <-ctx.Done():
			return &pb.DBUnFollowRsp{}, errors.New("timeout")
		case _, ok := <-rspChn:
			if !ok{
				rlog.Printf("<-rspChn not ok in follow. reqId=%d", reqId)
				return &pb.DBUnFollowRsp{}, errors.New("error")
			}
		}
	}

	return &pb.DBUnFollowRsp{}, nil
}
func (*serverT) Post(ctx context.Context, in *pb.DBPostReq) (*pb.DBPostRsp, error) {
	//先写content表
	{
		//为本次请求获取一个reqId
		reqId := gReqIdGen.getOneId()

		req := dbReqCmdT{reqId:reqId, cmd:dbCmdInsertMsgContent, req: in}

		//创建rsp管道
		rspChn := make(chan *dbRspCmdT, 1)
		gRspChanMap.insertChn(reqId, rspChn)
		//rsp后就扔掉
		defer gRspChanMap.delChn(reqId)

		//确定一个输送管道然后发送req
		colName := fmt.Sprintf("MsgContent_%d", in.Content.MsgId%int64(gConfig.ContentColCount))
		reqChn, ok := gColName2DBReqChnMap[colName]
		if !ok{
			rlog.Printf("gColName2DBReqChnMap[%s] not ok", colName)
			return &pb.DBPostRsp{}, errors.New("error")
		}
		reqChn<-&req

		//等待rsp或超时
		select {
		case <-ctx.Done():
			return &pb.DBPostRsp{}, errors.New("timeout")
		case _, ok := <-rspChn:
			if !ok{
				rlog.Printf("<-rspChn not ok in follow. reqId=%d", reqId)
				return &pb.DBPostRsp{}, errors.New("error")
			}
		}
	}
	//再写UserMsgId表
	{
		//为本次请求获取一个reqId
		reqId := gReqIdGen.getOneId()

		req := dbReqCmdT{reqId:reqId, cmd:dbCmdInsertUserMsgId, req: in}

		//创建rsp管道
		rspChn := make(chan *dbRspCmdT, 1)
		gRspChanMap.insertChn(reqId, rspChn)
		//rsp后就扔掉
		defer gRspChanMap.delChn(reqId)

		//确定一个输送管道然后发送req
		colName := fmt.Sprintf("UserMsgId_%d", in.Content.UserId%int64(gConfig.UserMsgIdColCount))
		reqChn, ok := gColName2DBReqChnMap[colName]
		if !ok{
			rlog.Printf("gColName2DBReqChnMap[%s] not ok", colName)
			return &pb.DBPostRsp{}, errors.New("error")
		}
		reqChn<-&req

		//等待rsp或超时
		select {
		case <-ctx.Done():
			return &pb.DBPostRsp{}, errors.New("timeout")
		case _, ok := <-rspChn:
			if !ok{
				rlog.Printf("<-rspChn not ok in follow. reqId=%d", reqId)
				return &pb.DBPostRsp{}, errors.New("error")
			}
		}
	}

	return &pb.DBPostRsp{}, nil
}


//gRpc proto end///////////////////////////////////////////



