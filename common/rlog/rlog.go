/*
@Author : Ryan.wuxiaoyong
*/

package rlog

import (
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"sync"
	"time"
)

func Init(myName string, svrAddr string){
	gMyName = myName
	gRlogSvrAddr = svrAddr

	go logRoutine()
	go fatalRoutine()
	go statRoutine()
	go warningRoutine()
}

func Printf(format string, v ...interface{}){
	log.Printf(format, v...)

	gLogChn.Lock()

	if len(gLogChn.chn) == cap(gLogChn.chn){
		gLogChn.Unlock()

		//日志太多了，发一条warning
		now := time.Now()
		if now.Sub(lastTooManyLogsTime) > time.Second*10{
			Warning(WarningLevelNormal, "too many logs.", []StatPointData{})
			lastTooManyLogsTime = now
		}

		return
	}
	gLogChn.chn <- fmt.Sprintf(format, v...)

	gLogChn.Unlock()
}

func Fatalf(format string, v ...interface{}){
	gFatalLogChn<- fmt.Sprintf(format, v...)

	select {
	case <-gFatalLogRspChn:
	case <-time.NewTimer(2*time.Second).C:
	}

	log.Fatalf(format, v...)
}

func Panicf(format string, v ...interface{}){
	gFatalLogChn<- fmt.Sprintf(format, v...)

	select {
	case <-gFatalLogRspChn:
	case <-time.NewTimer(2*time.Second).C:
	}

	log.Panicf(format, v...)
}

//状态数据打点
func StatPoint(pointId string, dataArr []StatPointData){
	var dataArrPB []*StatPointDataPB
	for _, data := range dataArr{
		d := StatPointDataPB{Name:data.Name, Data:data.Data}
		dataArrPB = append(dataArrPB, &d)
	}
	data := statPointData{id:pointId, dataArr:dataArrPB}

	gStatChn.Lock()
	defer gStatChn.Unlock()

	if len(gStatChn.chn) == cap(gStatChn.chn){
		return
	}
	gStatChn.chn<- data
}

func Warning(level int, desc string, dataArr []StatPointData){
	var dataArrPB []*StatPointDataPB
	for _, data := range dataArr{
		d := StatPointDataPB{Name:data.Name, Data:data.Data}
		dataArrPB = append(dataArrPB, &d)
	}
	data := warningData{level:level, desc:desc, dataArr:dataArrPB}

	gWarningChn.Lock()
	defer gWarningChn.Unlock()

	if len(gWarningChn.chn) == cap(gWarningChn.chn){
		return
	}
	gWarningChn.chn<- data
}


var lastTooManyLogsTime time.Time

type StatPointData struct{
	Name string
	Data int64
}

//告警级别
const(
	WarningLevelNormal = 1
	WarningLevelFatal = 9
)


//////////////////////////////////////////////////////

//每秒最多输出普通日志数量限制
const maxLogPerSec = 10
//普通日志队列缓存大小
const logChanCap = 100

//作为本进程的标识
var gMyName string
//rlog服务地址
var gRlogSvrAddr string

type logChan struct {
	sync.Mutex
	chn chan string
}
var gLogChn = logChan{sync.Mutex{}, make(chan string, logChanCap)} //一次性最多缓存100条

var gFatalLogChn = make(chan string, 1)
var gFatalLogRspChn = make(chan bool, 1)

type statPointData struct {
	id string
	dataArr []*StatPointDataPB
}
type statChan struct {
	sync.Mutex
	chn chan statPointData
}
var gStatChn = statChan{sync.Mutex{}, make(chan statPointData, 100)}

type warningData struct {
	level int
	desc string
	dataArr []*StatPointDataPB
}
type warningChan struct {
	sync.Mutex
	chn chan warningData
}
var gWarningChn = warningChan{sync.Mutex{}, make(chan warningData, 100)}

/////////////////////////////////////////////
type grpcConn struct {
	Conn *grpc.ClientConn
	Client interface{}
}

func logRoutine(){
	svrConn := grpcConn{}

	for{
		data := <-gLogChn.chn

		//允许重试一次
		for i:=0; i<2; i++{
			if svrConn.Conn == nil{
				//还未链接则链接
				conn, err := grpc.Dial(gRlogSvrAddr, grpc.WithInsecure())
				if err != nil {
					log.Printf("connect RlogSvr failed. [%+v]", err)
					//连不上，视为服务down掉，忽略
					continue
				}
				client := NewRlogSvrClient(conn)
				svrConn.Conn = conn
				svrConn.Client = client
			}

			//
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ , err := svrConn.Client.(RlogSvrClient).Log(ctx, &LogReq{MyName:gMyName, Log:data, TimeStamp:time.Now().Unix()})
			cancel()
			if err != nil {
				log.Printf("Rlog log failed. [%+v]", err)

				_ = svrConn.Conn.Close()
				svrConn.Conn = nil
				//重试一次
				continue
			}
			break
		}

		//限定普通日志吞吐速度
		time.Sleep(1/maxLogPerSec * time.Second)
	}
}
func fatalRoutine(){
	svrConn := grpcConn{}

	for{
		data := <-gFatalLogChn

		//允许重试一次
		for i:=0; i<2; i++{
			if svrConn.Conn == nil{
				//还未链接则链接
				conn, err := grpc.Dial(gRlogSvrAddr, grpc.WithInsecure())
				if err != nil {
					log.Printf("connect RlogSvr failed. [%+v]", err)
					//连不上，视为服务down掉，忽略
					continue
				}
				client := NewRlogSvrClient(conn)
				svrConn.Conn = conn
				svrConn.Client = client
			}

			//
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ , err := svrConn.Client.(RlogSvrClient).LogFatal(ctx, &LogFatalReq{MyName:gMyName, Log:data, TimeStamp:time.Now().Unix()})
			cancel()
			if err != nil {
				log.Printf("Rlog logFatal failed. [%+v]", err)

				_ = svrConn.Conn.Close()
				svrConn.Conn = nil
				//重试一次
				continue
			}

			//
			gFatalLogRspChn<- true
			break
		}
	}
}
func statRoutine(){
	svrConn := grpcConn{}

	for{
		data := <-gStatChn.chn

		//允许重试一次
		for i:=0; i<2; i++{
			if svrConn.Conn == nil{
				//还未链接则链接
				conn, err := grpc.Dial(gRlogSvrAddr, grpc.WithInsecure())
				if err != nil {
					log.Printf("connect RlogSvr failed. [%+v]", err)
					//连不上，视为服务down掉，忽略
					continue
				}
				client := NewRlogSvrClient(conn)
				svrConn.Conn = conn
				svrConn.Client = client
			}

			//
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ , err := svrConn.Client.(RlogSvrClient).StatPoint(ctx, &StatPointReq{MyName:gMyName, PointId:data.id, DataArr:data.dataArr, TimeStamp:time.Now().Unix()})
			cancel()
			if err != nil {
				log.Printf("Rlog StatPoint failed. [%+v]", err)

				_ = svrConn.Conn.Close()
				svrConn.Conn = nil
				//重试一次
				continue
			}
			break
		}
	}
}
func warningRoutine(){
	svrConn := grpcConn{}

	for{
		data := <-gWarningChn.chn

		//允许重试一次
		for i:=0; i<2; i++{
			if svrConn.Conn == nil{
				//还未链接则链接
				conn, err := grpc.Dial(gRlogSvrAddr, grpc.WithInsecure())
				if err != nil {
					log.Printf("connect RlogSvr failed. [%+v]", err)
					//连不上，视为服务down掉，忽略
					continue
				}
				client := NewRlogSvrClient(conn)
				svrConn.Conn = conn
				svrConn.Client = client
			}

			//
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ , err := svrConn.Client.(RlogSvrClient).Warning(ctx, &WarningReq{MyName:gMyName, Level:int32(data.level), Desc:data.desc, DataArr:data.dataArr, TimeStamp:time.Now().Unix()})
			cancel()
			if err != nil {
				log.Printf("Rlog Warning failed. [%+v]", err)

				_ = svrConn.Conn.Close()
				svrConn.Conn = nil
				//重试一次
				continue
			}
			break
		}
	}
}
