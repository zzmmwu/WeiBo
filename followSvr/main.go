/*
@Author : Ryan.wuxiaoyong
*/

package main


import (
	"WeiBo/common"
	"WeiBo/common/dbConnPool"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"sync/atomic"

	//"WeiBo/common"
	"context"
	"encoding/json"
	//"google.golang.org/grpc/keepalive"
	"io/ioutil"
	//"log"
	"net"
	"sync"
	"time"
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
	FollowColCount int `json: "followColCount"`
}
//配置文件数据对象
var gConfig = &configType{}

//db连接池
var gDBConnPool = dbConnPool.MongoConnPool{}

//用户关注数据map
const followHashMapSlotCount  = 1000000 //一百万slot
type followHashMapT struct {
	slots [followHashMapSlotCount]*followHashSlotT

	userIdCount int64
	followIdCount int64
}
//hash表slot
type followHashSlotT struct {
	sync.RWMutex

	userIdMap map[int64]map[int64]followInfoT
}
//关注信息
type followInfoT struct {
	followTime int64 //关注时间戳，unixtime
}
var gFollowHashMap = followHashMapT{}
func (m *followHashMapT)init(){
	for i := range m.slots{
		m.slots[i] = &followHashSlotT{sync.RWMutex{}, map[int64]map[int64]followInfoT{}}
	}

	m.userIdCount = 0
	m.followIdCount = 0
}
func (m *followHashMapT)getFollowArr(ctx context.Context, userId int64) []int64{
	slot := m.slots[userId%followHashMapSlotCount]

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
		atomic.AddInt64(&m.followIdCount, int64(len(followMap)))

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
func (m *followHashMapT)loadUserFollowFromDB(ctx context.Context, userId int64) (map[int64]followInfoT, bool){
	client, err := gDBConnPool.WaitForOneConn(ctx)
	if err != nil{
		rlog.Printf("gDBConnPool.WaitForOneConn failed. err=[%+v]", err)
		return nil, false
	}
	defer gDBConnPool.ReturnConn(client)

	//分表后的colName
	colName := fmt.Sprintf("Follow_%d", userId%int64(gConfig.FollowColCount))
	collection := client.Database(gConfig.MongoDBName).Collection(colName)
	//rlog.Printf("colName:%+s userId:%d", colName, userId)

	var data common.DBFollow
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
	followMap := map[int64]followInfoT{}
	for cursor.Next(ctx){
		err = cursor.Decode(&data)
		if err != nil {
			rlog.Printf("[%+v]", err)
			return nil, false
		}
		//rlog.Printf("cursor:[%+v", cursor)
		//rlog.Printf("DBFollow data:[%+v", data)
		followMap[data.FollowId] = followInfoT{followTime:data.FollowTime}
	}

	return followMap, true
}
func (m *followHashMapT)follow(userId int64, followedId int64){
	slot := m.slots[userId%followHashMapSlotCount]

	slot.Lock()
	defer slot.Unlock()

	followMap, ok := slot.userIdMap[userId]
	if !ok{
		//如果没有这个user则说明该user还未从数据库加载，这里无需做处理。等待getFollowArr时从DB完整加载
		////如果没有这个user则创建一个
		//followMap = make(map[int64]followInfoT)
		//slot.userIdMap[userId] = followMap
	}else{
		followMap[followedId] = followInfoT{followTime:time.Now().Unix()}

		//计数
		atomic.AddInt64(&m.followIdCount, 1)
	}
}
func (m *followHashMapT)unFollow(userId int64, followedId int64){
	slot := m.slots[userId%followHashMapSlotCount]

	slot.Lock()
	defer slot.Unlock()

	followMap := slot.userIdMap[userId]
	delete(followMap, followedId)

	//计数
	atomic.AddInt64(&m.followIdCount, -1)
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
	gFollowHashMap.init()

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
	pb.RegisterFollowSvrServer(grpcServer, &serverT{})
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
			data := rlog.StatPointData{Name:"cacheUserCount", Data:gFollowHashMap.userIdCount}
			dataArr = append(dataArr, data)

			data = rlog.StatPointData{Name:"cacheFollowCount", Data:gFollowHashMap.followIdCount}
			dataArr = append(dataArr, data)
		}
		rlog.StatPoint("totalReport", dataArr)
	}
}

//gRpc proto///////////////////////////////////////////
type serverT struct{}

func (s *serverT) QueryFollowList(ctx context.Context, in *pb.QueryFollowListReq) (*pb.QueryFollowListRsp, error) {
	followArr := gFollowHashMap.getFollowArr(ctx, in.UserId)
	return &pb.QueryFollowListRsp{FollowIdArr:followArr}, nil
}
func (s *serverT) Follow(ctx context.Context, in *pb.FollowReq) (*pb.FollowRsp, error) {
	gFollowHashMap.follow(in.UserId, in.FollowedUserId)
	return &pb.FollowRsp{}, nil
}
func (s *serverT) UnFollow(ctx context.Context, in *pb.UnFollowReq) (*pb.UnFollowRsp, error) {
	gFollowHashMap.unFollow(in.UserId, in.UnFollowedUserId)
	return &pb.UnFollowRsp{}, nil
}

//gRpc proto end///////////////////////////////////////////



