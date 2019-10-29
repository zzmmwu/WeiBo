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
	"io/ioutil"
	"sync/atomic"
	"time"

	//"log"
	"net"
	"sort"
	"strconv"
	"sync"
	//"time"

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
	UserMsgIdColCount int `json: "userMsgIdColCount"`
}
//配置文件数据对象
var gConfig = &configType{}


//
//用户msg数据map
const userHashMapSlotCount  = 1000000 //一百万slot
type userMsgHashMapT struct {
	slots [userHashMapSlotCount]*userMsgHashSlotT

	userCounter int64
	msgCounter int64
}
//hash表slot
type userMsgHashSlotT struct {
	sync.RWMutex

	//value为msgId的slice，按照msgId从小到大排列
	userIdMap map[int64][]int64
}
var gUserMsgHashMap = userMsgHashMapT{}

//db连接池
var gDBConnPool = dbConnPool.MongoConnPool{}

func (m *userMsgHashMapT)init(){
	for i := range m.slots{
		m.slots[i] = &userMsgHashSlotT{sync.RWMutex{}, map[int64][]int64{}}
	}

	m.userCounter = 0
	m.msgCounter = 0
}
func (m *userMsgHashMapT)pull(ctx context.Context, userId int64, lastMsgId int64) []int64{
	slot := m.slots[userId%userHashMapSlotCount]

	//
	slot.RLock()

	totalMsgArr, ok := slot.userIdMap[userId]
	if !ok{
		//释放锁
		slot.RUnlock()

		//user还未加载，从DB加载
		totalMsgArr, ok = m.loadUserMsgIdFromDB(ctx, userId)
		if !ok{
			return []int64{}
		}
		//计数
		atomic.AddInt64(&m.userCounter, 1)
		atomic.AddInt64(&m.msgCounter, int64(len(totalMsgArr)))

		//写锁
		slot.Lock()
		//
		slot.userIdMap[userId] = totalMsgArr
		//
		slot.Unlock()

		//重新锁住
		slot.RLock()
	}

	//寻找第一个比lastMsgId小的
	var finalMsgArr []int64
	if lastMsgId == 0{
		//为0表示获取最新的
		finalMsgArr = totalMsgArr
	}else{
		//rlog.Printf("totalMsgArr org =%v", totalMsgArr)
		for i:=len(totalMsgArr)-1; i>=0; i--{
			if totalMsgArr[i] < lastMsgId{
				finalMsgArr = totalMsgArr[:i+1]
				break
			}
		}
		//rlog.Printf("finalMsgArr new =%v", finalMsgArr)
	}
	//rlog.Printf("finalMsgArr new =%v", finalMsgArr)

	//选其中msdId最大的xxx个
	if len(finalMsgArr) > common.MaxMsgCountInPull {
		finalMsgArr = finalMsgArr[len(totalMsgArr)-common.MaxMsgCountInPull:]
	}

	slot.RUnlock()

	return finalMsgArr
}
//返回userId对应的msgIdSlice，如果返回的ok不为true则表示userId不存在
func (m *userMsgHashMapT)loadUserMsgIdFromDB(ctx context.Context, userId int64) ([]int64, bool){
	client, err := gDBConnPool.WaitForOneConn(ctx)
	if err != nil{
		rlog.Printf("gDBConnPool.WaitForOneConn failed. err=[%+v]", err)
		return []int64{}, false
	}
	defer gDBConnPool.ReturnConn(client)

	//分表后的colName
	colName := "UserMsgId_"+strconv.Itoa(int(userId%int64(gConfig.UserMsgIdColCount)))
	collection := client.Database(gConfig.MongoDBName).Collection(colName)

	var userMsgId common.DBUserMsgId
	filter := bson.D{{"userid", userId}}
	cursor, err := collection.Find(ctx, filter)
	if err != nil{
		rlog.Printf("find userid=[%d] failed. err=[%+v]", userId, err)
		return []int64{}, false
	}
	if cursor.Err() != nil{
		rlog.Printf("find userid=[%d] failed. err=[%+v]", userId, cursor.Err())
		return []int64{}, false
	}
	//
	var msgIdArr []int64
	for cursor.Next(ctx){
		err = cursor.Decode(&userMsgId)
		if err != nil {
			rlog.Printf("[%+v]", err)
			return []int64{}, false
		}
		msgIdArr = append(msgIdArr, userMsgId.MsgId)
	}

	return msgIdArr, true

	//if userId == 111 {
	//	return []int64{3,4,6,9,30}, true
	//}
	//if userId == 222 {
	//	return []int64{2,8,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25}, true
	//}
	//if userId == 333 {
	//	return []int64{1,5,7}, true
	//}
	//return []int64{}, true
}
func (m *userMsgHashMapT)post(userId int64, msgId int64){
	slot := m.slots[userId%userHashMapSlotCount]

	slot.Lock()
	defer slot.Unlock()

	_, ok := slot.userIdMap[userId]
	//rlog.Printf("post totalMsgArr=%+v", slot.userIdMap[userId])
	if ok{
		//按照从小到大插入
		slot.userIdMap[userId] = append(slot.userIdMap[userId], msgId)
		for i:=len(slot.userIdMap[userId])-2; i>=0; i--{
			if slot.userIdMap[userId][i] > msgId {
				temp := slot.userIdMap[userId][i]
				slot.userIdMap[userId][i] = msgId
				slot.userIdMap[userId][i+1] = temp
			}else{
				break
			}
		}

		//计数
		atomic.AddInt64(&m.msgCounter, 1)
	}else{
		//如果没有这个user则说明该user还未从数据库加载，这里无需做处理。等待pull时从DB完整加载
	}
}
func (m *userMsgHashMapT)deleteMsg(userId int64, msgId int64){
	slot := m.slots[userId%userHashMapSlotCount]

	slot.Lock()
	defer slot.Unlock()

	totalMsgArr, ok := slot.userIdMap[userId]
	if !ok{
		return
	}
	for i:=len(totalMsgArr)-1; i>=0; i--{
		if totalMsgArr[i] == msgId {
			copy(totalMsgArr[:i], totalMsgArr[i+1:])
			totalMsgArr = totalMsgArr[:len(totalMsgArr)-1]

			//计数
			atomic.AddInt64(&m.msgCounter, -1)
			break


		}
	}
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
	gUserMsgHashMap.init()

	//开启数据打点routine
	go statReportRoutine()

	//开启grpc服务
	lis, err := net.Listen("tcp", gConfig.ListenAddr)
	if err != nil {
		rlog.Fatalf("failed to listen: %+v", err)
	}
	rlog.Printf("begin to Listen [%s]", gConfig.ListenAddr)
	grpcServer := grpc.NewServer()
	pb.RegisterUserMsgIdSvrServer(grpcServer, &serverT{})
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
			data := rlog.StatPointData{Name:"cacheUserCount", Data:gUserMsgHashMap.userCounter}
			dataArr = append(dataArr, data)

			data = rlog.StatPointData{Name:"cacheMsgIdCount", Data:gUserMsgHashMap.msgCounter}
			dataArr = append(dataArr, data)
		}
		rlog.StatPoint("totalReport", dataArr)
	}
}

//gRpc proto///////////////////////////////////////////
type serverT struct{}

func (s *serverT) PullMsg(ctx context.Context, in *pb.PullMsgIdReq) (*pb.PullMsgIdRsp, error) {
	var totalMsgArr []int64
	for _, userId := range in.UserIdArr{
		msgArr := gUserMsgHashMap.pull(ctx, userId, in.LastMsgId)
		totalMsgArr = append(totalMsgArr, msgArr...)
	}
	//排序
	sort.Slice(totalMsgArr, func(i, j int)bool{return totalMsgArr[i] < totalMsgArr[j]})
	//选id最大的xx条
	if len(totalMsgArr)-common.MaxMsgCountInPull > 0{
		totalMsgArr = totalMsgArr[len(totalMsgArr)-common.MaxMsgCountInPull:]
	}

	return &pb.PullMsgIdRsp{MsgIdArr:totalMsgArr}, nil
}
func (s *serverT) PostMsg(ctx context.Context, in *pb.PostMsgIdReq) (*pb.PostMsgIdRsp, error) {
	gUserMsgHashMap.post(in.UserId, in.MsgId)
	return &pb.PostMsgIdRsp{}, nil
}
func (s *serverT) DeleteMsg(ctx context.Context, in *pb.DeleteMsgReq) (*pb.DeleteMsgRsp, error) {
	gUserMsgHashMap.deleteMsg(in.UserId, in.MsgId)
	return &pb.DeleteMsgRsp{}, nil
}

//gRpc proto end///////////////////////////////////////////



