/*
@Author : Ryan.wuxiaoyong
*/

package main


import (
	"WeiBo/common"
	"WeiBo/common/dbConnPool"
	"context"
	"encoding/json"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"io/ioutil"
	"sync/atomic"

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
	MyName string `json: "myName"`
	RlogSvrAddr string `json: "rlogSvrAddr"`
	ListenAddr string  `json: "listenAddr"`
	MongoDBUrl string `json: "mongoDBUrl"`
	MongoDBName string `json: "mongoDBName"`
	UserLevelColCount int `json: "userLevelColCount"`
	FollowedColCount int `json: "followedColCount"`
}
//配置文件数据对象
var gConfig = &configType{}

//db连接池
var gDBConnPool = dbConnPool.MongoConnPool{}

//
//用户关注数据map
const followedHashMapSlotCount  = 1000000 //一百万slot
type followedHashMapT struct {
	slots [followedHashMapSlotCount]*followedHashSlotT

	userIdCount int64
	followerIdCount int64
}
//hash表slot
type followedHashSlotT struct {
	sync.RWMutex

	userIdMap map[int64]map[int64]followedInfoT
}
//关注信息
type followedInfoT struct {
	followTime int64 //关注时间戳，unixtime
}
var gFollowedHashMap = followedHashMapT{}
func (m *followedHashMapT)init(){
	for i := range m.slots{
		m.slots[i] = &followedHashSlotT{sync.RWMutex{}, map[int64]map[int64]followedInfoT{}}
	}

	m.userIdCount = 0
	m.followerIdCount = 0
}
func (m *followedHashMapT)getFollowArr(ctx context.Context, userId int64) []int64{
	slot := m.slots[userId%followedHashMapSlotCount]

	slot.RLock()

	followMap, ok := slot.userIdMap[userId]
	if !ok {
		//释放锁
		slot.RUnlock()

		//user还未加载，从DB加载
		followMap, ok = m.loadUserFollowFromDB(ctx, userId)
		if !ok{
			return []int64{}
		}
		//计数
		atomic.AddInt64(&m.userIdCount, 1)
		atomic.AddInt64(&m.followerIdCount, int64(len(followMap)))

		//写锁
		slot.Lock()
		//
		slot.userIdMap[userId] = followMap
		//
		slot.Unlock()

		//重新锁住
		slot.RLock()
	}
	var idArr []int64
	for id := range followMap{
		idArr = append(idArr, id)
	}

	slot.RUnlock()

	return idArr
}
//返回userId对应的msgIdSlice，如果返回的ok不为true则表示userId不存在
func (m *followedHashMapT)loadUserFollowFromDB(ctx context.Context, userId int64) (map[int64]followedInfoT, bool){
	client, err := gDBConnPool.WaitForOneConn(ctx)
	if err != nil{
		rlog.Printf("gDBConnPool.WaitForOneConn failed. err=[%+v]", err)
		return nil, false
	}
	defer gDBConnPool.ReturnConn(client)

	///先查询followedColName
	followedColName := ""
	{
		filter := bson.D{{"userid", userId}}
		colName := "UserLevel_" + fmt.Sprintf("%d", userId%int64(gConfig.UserLevelColCount))
		collection := client.Database(gConfig.MongoDBName).Collection(colName)
		result := collection.FindOne(ctx, filter)
		if result.Err() == mongo.ErrNoDocuments {
			//按level 0
			followedColName = "Followed_" + fmt.Sprintf("0_%d", userId%int64(gConfig.FollowedColCount))
		}else{
			if result.Err() != mongo.ErrNoDocuments && result.Err() != nil {
				rlog.Printf("[%+v]", result.Err())
				return nil, false
			}
			var data common.DBUserLevel
			err = result.Decode(&data)
			if err != nil {
				rlog.Printf("[%+v]", err)
				return nil, false
			}

			//
			if data.Level < common.UserLevelSuper{
				followedColName = "Followed_" + fmt.Sprintf("%d_%d", data.Level, userId%int64(gConfig.FollowedColCount))
			}else{
				//大V，单独一张表
				followedColName = common.FollowedColNameOfVIP(userId)
			}
		}
	}

	//
	collection := client.Database(gConfig.MongoDBName).Collection(followedColName)
	var data common.DBFollowed
	filter := bson.D{{"userid", userId}}
	cursor, err := collection.Find(ctx, filter)
	if err != nil{
		rlog.Printf("find userid=[%d] failed. err=[%+v]", userId, err)
		return nil, false
	}
	if cursor.Err() != nil{
		rlog.Printf("find userid=[%d] failed. err=[%+v]", userId, cursor.Err())
		return nil, false
	}
	//
	followMap := map[int64]followedInfoT{}
	for cursor.Next(ctx){
		err = cursor.Decode(&data)
		if err != nil {
			rlog.Printf("[%+v]", err)
			return nil, false
		}
		followMap[data.FollowedId] = followedInfoT{followTime:data.FollowTime}
	}

	return followMap, true
}
func (m *followedHashMapT)follow(userId int64, followedId int64){
	//rlog.Printf("slots len=[%d]", len(m.slots))
	slot := m.slots[followedId%followedHashMapSlotCount]
	//rlog.Printf("slots[1]=%+v", slot)

	slot.Lock()
	defer slot.Unlock()

	followMap, ok := slot.userIdMap[followedId]
	if !ok{
		//如果没有这个user则说明该user还未从数据库加载，这里无需做处理。等待getFollowArr时从DB完整加载
		////如果没有这个user则创建一个
		//followMap = make(map[int64]followInfoT)
		//slot.userIdMap[userId] = followMap
	}else{
		followMap[userId] = followedInfoT{followTime:time.Now().Unix()}
		//rlog.Printf("followMap=[%+v", followMap)
		//计数
		atomic.AddInt64(&m.followerIdCount, 1)
	}

}
func (m *followedHashMapT)unFollow(userId int64, followedId int64){
	slot := m.slots[followedId%followedHashMapSlotCount]

	slot.Lock()
	defer slot.Unlock()

	followMap := slot.userIdMap[followedId]
	delete(followMap, userId)

	//计数
	atomic.AddInt64(&m.followerIdCount, -1)
}


//onlineUser到FrontNotifySvr的数据结构
type userIdOnlineT struct{
	sync.RWMutex

	dataMap map[int64]bool
}
var gUserIdOnlineMap = userIdOnlineT{sync.RWMutex{}, map[int64]bool{}}
func (m *userIdOnlineT)store(userId int64){
	m.Lock()
	defer m.Unlock()

	m.dataMap[userId] = true
}
func (m *userIdOnlineT)delete(userId int64){
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

	//
	err = gDBConnPool.Init(200, gConfig.MongoDBUrl, "", "")
	if err != nil{
		rlog.Fatalf("%+v", err)
	}
	rlog.Printf("dbpool init successed")

	//
	gFollowedHashMap.init()

	//开启数据打点routine
	go statReportRoutine()

	//开启grpc服务
	lis, err := net.Listen("tcp", gConfig.ListenAddr)
	if err != nil {
		rlog.Fatalf("failed to listen: %+v", err)
	}
	rlog.Printf("begin to Listen [%s]", gConfig.ListenAddr)
	grpcServer := grpc.NewServer()
	pb.RegisterFollowedSvrServer(grpcServer, &serverT{})
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
			data := rlog.StatPointData{Name:"cacheUserCount", Data:gFollowedHashMap.userIdCount}
			dataArr = append(dataArr, data)

			data = rlog.StatPointData{Name:"cacheFollowerCount", Data:gFollowedHashMap.followerIdCount}
			dataArr = append(dataArr, data)

			gUserIdOnlineMap.RLock()
			onlineCount := len(gUserIdOnlineMap.dataMap)
			gUserIdOnlineMap.RUnlock()
			data = rlog.StatPointData{Name:"onlineUserCount", Data:int64(onlineCount)}
			dataArr = append(dataArr, data)
		}
		rlog.StatPoint("totalReport", dataArr)
	}
}

//gRpc proto///////////////////////////////////////////
type serverT struct{}

func (s *serverT) QueryFollowedList(ctx context.Context, in *pb.QueryFollowedListReq) (*pb.QueryFollowedListRsp, error) {
	followedArr := gFollowedHashMap.getFollowArr(ctx, in.UserId)
	return &pb.QueryFollowedListRsp{FollowedIdArr:followedArr}, nil
}
func (s *serverT) QueryOnlineFollowedList(ctx context.Context, in *pb.QueryFollowedListReq) (*pb.QueryFollowedListRsp, error) {
	followedArr := gFollowedHashMap.getFollowArr(ctx, in.UserId)
	//online过滤
	var onlineArr []int64
	gUserIdOnlineMap.RLock()
	for _, userId := range followedArr{
		_, ok := gUserIdOnlineMap.dataMap[userId]
		if ok {
			onlineArr = append(onlineArr, userId)
		}
	}
	gUserIdOnlineMap.RUnlock()

	return &pb.QueryFollowedListRsp{FollowedIdArr:onlineArr}, nil
}
func (s *serverT) Follow(ctx context.Context, in *pb.FollowReq) (*pb.FollowRsp, error) {
	gFollowedHashMap.follow(in.UserId, in.FollowedUserId)
	return &pb.FollowRsp{}, nil
}
func (s *serverT) UnFollow(ctx context.Context, in *pb.UnFollowReq) (*pb.UnFollowRsp, error) {
	gFollowedHashMap.unFollow(in.UserId, in.UnFollowedUserId)
	return &pb.UnFollowRsp{}, nil
}

func (*serverT) Online(ctx context.Context, req *pb.OnlineReq) (*pb.OnlineRsp, error) {
	gUserIdOnlineMap.store(req.UserId)
	return &pb.OnlineRsp{}, nil
}
func (*serverT) Offline(ctx context.Context, req *pb.OfflineReq) (*pb.OfflineRsp, error) {
	gUserIdOnlineMap.delete(req.UserId)
	return &pb.OfflineRsp{}, nil
}


//gRpc proto end///////////////////////////////////////////



