/*
@Author : Ryan.wuxiaoyong
*/

package main


import (
	"WeiBo/common/dbSvrConnPool"
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
	MyName         string `json: "myName"`
	RlogSvrAddr    string `json: "rlogSvrAddr"`
	ListenAddr     string `json: "listenAddr"`
	DBSvrAddr         string `json: "dbSvrAddr"`
}
//配置文件数据对象
var gConfig = &configType{}

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
	connData, err := gDBConnPool.WaitForOneConn(ctx)
	if err != nil{
		rlog.Printf("gDBConnPool.WaitForOneConn failed. err=[%+v]", err)
		return map[int64]followInfoT{}, false
	}
	defer gDBConnPool.ReturnConn(connData)
	dbSvrConn := connData.Conn
	dbSvrClient := connData.Client
	
	//允许重试一次
	for i:=0; i<2; i++{
		if dbSvrConn == nil{
			var err interface{}
			dbSvrConn, err = grpc.Dial(gConfig.DBSvrAddr, grpc.WithInsecure())
			if err != nil {
				rlog.Printf("connect frontSvr failed [%+v]", err)
				break
			}
			dbSvrClient = pb.NewDbSvrClient(dbSvrConn)
		}
		rsp, err := dbSvrClient.QueryFollow(ctx, &pb.DBQueryFollowReq{UserId:userId})
		if err != nil{
			//重连一下
			_ = dbSvrConn.Close()
			dbSvrConn = nil
			continue
		}

		//
		followMap := map[int64]followInfoT{}
		for _, followerId := range rsp.FollowIdArr{
			followMap[followerId] =followInfoT{}
		}

		return followMap, true
	}

	return map[int64]followInfoT{}, false
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

//db连接池
var gDBConnPool = dbSvrConnPool.DBSvrConnPool{}

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
	err = gDBConnPool.Init(200, gConfig.DBSvrAddr)
	if err != nil{
		rlog.Fatalf("%+v", err)
	}
	rlog.Printf("dbPool init success")

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



