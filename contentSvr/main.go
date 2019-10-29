/*
@Author : Ryan.wuxiaoyong
*/

package main


import (
	"WeiBo/common"
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	//"errors"
	"go.mongodb.org/mongo-driver/bson"
	"google.golang.org/grpc"
	"io/ioutil"
	//"log"
	"net"
	"os"
	"strconv"
	"sync"

	"WeiBo/common/rlog"
	"WeiBo/common/dbConnPool"
	pb "WeiBo/common/protobuf"
)

//配置文件格式
type configType struct {
	MyName string `json: "myName"`
	RlogSvrAddr string `json: "rlogSvrAddr"`
	ListenAddr string  `json: "listenAddr"`
	MongoDBUrl string `json: "mongoDBUrl"`
	MongoDBName string `json: "mongoDBName"`
	ContentColCount int `json: "contentColCount"`
}
//配置文件数据对象
var gConfig = &configType{}


//
//msg数据map
const msgHashMapSlotCount  = 1000000 //一百万slot
type msgHashMapT struct {
	slots [msgHashMapSlotCount]*msgHashSlotT

	msgCounter int64
}
//hash表slot
type msgHashSlotT struct {
	sync.RWMutex

	//
	msgMap map[int64]*pb.MsgData
}
var gMsgHashMap = msgHashMapT{}

//db连接池
var gDBConnPool = dbConnPool.MongoConnPool{}

//
func (m *msgHashMapT)init(){
	for i := range m.slots{
		m.slots[i] = &msgHashSlotT{sync.RWMutex{}, map[int64]*pb.MsgData{}}
	}

	m.msgCounter = 0
}
func (m *msgHashMapT)pull(ctx context.Context, msgId int64) (*pb.MsgData, bool){
	slot := m.slots[msgId%msgHashMapSlotCount]

	//
	slot.RLock()

	msgData, ok := slot.msgMap[msgId]
	if !ok{
		//释放锁
		slot.RUnlock()

		//msg还未加载，从DB加载
		msgData, ok = m.loadMsgFromDB(ctx, msgId)
		if !ok{
			return nil, false
		}
		//计数
		atomic.AddInt64(&m.msgCounter, 1)

		//写锁
		slot.Lock()
		//
		slot.msgMap[msgId] = msgData
		//
		slot.Unlock()

		//重新锁住
		slot.RLock()
	}

	slot.RUnlock()

	return msgData, true
}
//如果返回的ok不为true则表示msgId不存在或db异常
func (m *msgHashMapT)loadMsgFromDB(ctx context.Context, msgId int64) (*pb.MsgData, bool){
	client, err := gDBConnPool.WaitForOneConn(ctx)
	if err != nil{
		rlog.Printf("gDBConnPool.WaitForOneConn failed. err=[%+v]", err)
		return nil, false
	}
	defer gDBConnPool.ReturnConn(client)

	//分表后的colName
	colName := "MsgContent_"+strconv.Itoa(int(msgId%int64(gConfig.ContentColCount)))
	collection := client.Database(gConfig.MongoDBName).Collection(colName)
	//log.Printf("find MsgId=%d, colName=%s", msgId, colName)

	var content common.DBMsgContent
	filter := bson.D{{"msgid", msgId}}
	result := collection.FindOne(ctx, filter)
	if result.Err() != nil{
		rlog.Printf("[%+v]", result.Err())
		return nil, false
	}
	err = result.Decode(&content)
	if err != nil {
		rlog.Printf("[%+v]", err)
		return nil, false
	}

	return &pb.MsgData{MsgId:content.MsgId, Text:content.Text, VideoUrl:content.VideoUrl, ImgUrlArr:content.ImgUrlArr}, true
}
//这里无需post操作，pull发现没有此msg则会从DB加载
//test
//func (m *msgHashMapT)post(content *pb.MsgData){
//	slot := m.slots[content.MsgId%msgHashMapSlotCount]
//
//	slot.Lock()
//	defer slot.Unlock()
//
//	log.Printf("post slot.msgMap=%+v", slot.msgMap)
//	slot.msgMap[content.MsgId] = content
//	log.Printf("post slot.msgMap=%+v", slot.msgMap)
//}
func (m *msgHashMapT)deleteMsg(msgId int64){
	slot := m.slots[msgId%msgHashMapSlotCount]

	slot.Lock()
	defer slot.Unlock()

	delete(slot.msgMap, msgId)
	//计数
	atomic.AddInt64(&m.msgCounter, -1)
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
	gMsgHashMap.init()

	//开启数据打点routine
	go statReportRoutine()

	//开启grpc服务
	lis, err := net.Listen("tcp", gConfig.ListenAddr)
	if err != nil {
		rlog.Fatalf("failed to listen: %+v", err)
	}
	rlog.Printf("begin to Listen [%s]", gConfig.ListenAddr)
	grpcServer := grpc.NewServer()
	pb.RegisterContentSvrServer(grpcServer, &serverT{})
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
			data := rlog.StatPointData{Name:"cacheMsgCount", Data:gMsgHashMap.msgCounter}
			dataArr = append(dataArr, data)
		}
		rlog.StatPoint("totalReport", dataArr)
	}
}

//gRpc proto///////////////////////////////////////////
type serverT struct{}

func (s *serverT) PullMsg(ctx context.Context, in *pb.PullMsgContentReq) (*pb.PullMsgContentRsp, error) {
	var contentArr []*pb.MsgData
	for _, msgId := range in.MsgIdArr{
		content, ok := gMsgHashMap.pull(ctx, msgId)
		if ok{
			contentArr = append(contentArr, content)
		}
	}

	return &pb.PullMsgContentRsp{MsgArr:contentArr}, nil
}
func (s *serverT) PostMsg(ctx context.Context, in *pb.PostMsgContentReq) (*pb.PostMsgContentRsp, error) {
	//这里无需post操作，pull发现没有此msg则会从DB加载
	//test
	//gMsgHashMap.post(in.Content)
	return &pb.PostMsgContentRsp{}, nil
}
func (s *serverT) DeleteMsg(ctx context.Context, in *pb.DeleteMsgReq) (*pb.DeleteMsgRsp, error) {
	gMsgHashMap.deleteMsg(in.MsgId)
	return &pb.DeleteMsgRsp{}, nil
}

//gRpc proto end///////////////////////////////////////////



