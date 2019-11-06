/*
@Author : Ryan.wuxiaoyong
*/

package main


import (
	"context"
	"math/rand"
	"sync"

	"os"
	"strconv"
	"time"
	"log"

	"google.golang.org/grpc"
	pb "WeiBo/common/protobuf"
	//"WeiBo/common/rlog"
)


type offlineUserIdCtrlT struct {
	sync.Mutex

	offLineUserIdMap []int64
}
var gOfflineUserIdCtrl = offlineUserIdCtrlT{}


var gFrontMngAddr string
var gFrontNotifyMngAddr string
var gStartUserId int64
var gEndUserId int64
var gOnlineCount int
var gPullDur int
var gPostPerSec1KOnlineUser int

var gPostWaitChan []chan int

func main(){


	if len(os.Args)!=8 {
		log.Fatalf("xxx frontSvrAddr frontNotifySvrAddr startUserId endUserId onlineCount pullDur postPerSec1KOnlineUser")
	}
	gFrontMngAddr = os.Args[1]
	gFrontNotifyMngAddr = os.Args[2]
	gStartUserId, _ = strconv.ParseInt(os.Args[3], 10, 64)
	gEndUserId, _ = strconv.ParseInt(os.Args[4], 10, 64)
	temp, _ := strconv.ParseInt(os.Args[5], 10, 64);
	gOnlineCount = int(temp)
	temp, _ = strconv.ParseInt(os.Args[6], 10, 64)
	gPullDur = int(temp)
	temp, _ = strconv.ParseInt(os.Args[7], 10, 64)
	gPostPerSec1KOnlineUser = int(temp)

	if gOnlineCount > int(gEndUserId-gStartUserId+1){
		log.Fatalf("onlineCount > (endUserId-startUserId)")
	}
	for i:= gStartUserId+int64(gOnlineCount); i<=gEndUserId; i++{
		gOfflineUserIdCtrl.offLineUserIdMap = append(gOfflineUserIdCtrl.offLineUserIdMap, i)
	}
	var tempCount = 0
	for i:=0; i<gOnlineCount; i++{
		go clientRountine(gStartUserId + int64(i))
		tempCount++
		if tempCount == 100{
			tempCount = 0
			time.Sleep(1*time.Second)
		}
	}

	//post
	for{
		time.Sleep(1*time.Second)
		//
		postCount := int(gOnlineCount/1000*gPostPerSec1KOnlineUser)
		if postCount == 0{
			postCount = 1
		}
		for i:=0; i<postCount; i++{
			index := rand.Intn(len(gPostWaitChan))
			gPostWaitChan[index]<- 1
		}
	}

	for{
		time.Sleep(100*time.Hour)
	}

}

func clientRountine(userId int64){
	//post等待管道
	postWaitChan := make(chan int, 1)
	gPostWaitChan = append(gPostWaitChan, postWaitChan)

	for{
		//退出同步管道
		routineEndChan := make(chan int, 3)

		//
		notifyConn, notifyClient, err := connNotifySvr(userId)
		if err != nil{
			log.Printf("connNotifySvr failed. err=[%v]", err)
			return
		}
		//心跳
		go func(){
			for{
				timer := time.NewTimer(30*time.Second)
				select {
				case <-routineEndChan:
					timer.Stop()
					return
				case <-timer.C:
					_, _ =notifyClient.HeartBeat(context.TODO(), &pb.CHeartBeatReq{UserId:userId})
					break
				}
			}
		}()
		//推送接受
		go func() {
			stream, err := notifyClient.CreateNotifyStream(context.TODO(), &pb.CCreateNotifyStreamReq{UserId: userId})
			if err != nil{
				log.Printf("CreateNotifyStream failed. err=[%+v]", err)
				return
			}
			for {
				notify, err := stream.Recv()
				if err != nil {
					//log.Printf("notify recv failed. err=[%+v]", err)
					return
				}
				log.Printf("notify recved. [%+v]", notify)
			}
		}()

		//
		frontConn, frontClient, err := connFrontSvr(userId)
		if err != nil{
			log.Printf("connFrontSvr failed. err=[%v]", err)
			return
		}
		//pull
		go func(){
			for{
				dur := rand.Intn(gPullDur+1) + gPullDur/2
				timer := time.NewTimer(time.Duration(dur)*time.Second)
				select {
				case <-routineEndChan:
					timer.Stop()
					return
				case <-timer.C:
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					_, err := frontClient.Pull(ctx, &pb.CPullReq{UserId:userId, LastMsgId:0})
					cancel()
					if err != nil{
						log.Printf("pull failed. [%v]", err)
						continue
					}
					//log.Printf("rsp:[%+v]\n", rsp)
					break
				}
			}
		}()
		//post
		go func(){
			for{
				select {
				case <-routineEndChan:
					return
				case <-postWaitChan:
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					rsp, err := frontClient.Post(ctx, &pb.CPostReq{UserId:userId, Msg:&pb.MsgData{MsgId:0, UserId:userId, Text:"hello", ImgUrlArr:[]string{"img0", "img1"}, VideoUrl:"vvv"}})
					cancel()
					if err != nil{
						log.Printf("post failed. [%+v]\n", err)
						continue
					}
					log.Printf("userId=%d post rsp:[%+v]\n", userId, rsp)
					break
				}
			}
		}()


		//下线定时
		//随机一个在线时长
		onlineTime := time.Duration(rand.Intn(10*60) + 60)*time.Second
		offlineTimer := time.NewTimer(onlineTime)
		select {
		case <-offlineTimer.C:
			routineEndChan<- 1
			routineEndChan<- 1
			routineEndChan<- 1
			_ = notifyConn.Close()
			_ = frontConn.Close()
			break
		}

		//userId归还
		//从新获取一个userId
		gOfflineUserIdCtrl.Lock()
		gOfflineUserIdCtrl.offLineUserIdMap = append(gOfflineUserIdCtrl.offLineUserIdMap, userId)
		userId = gOfflineUserIdCtrl.offLineUserIdMap[0]
		gOfflineUserIdCtrl.offLineUserIdMap = gOfflineUserIdCtrl.offLineUserIdMap[1:len(gOfflineUserIdCtrl.offLineUserIdMap)]
		gOfflineUserIdCtrl.Unlock()
	}
}

func connNotifySvr(userId int64) (*grpc.ClientConn, pb.FrontNotifySvrClient, error){
	//连接frontMng获取frontSvr地址
	frontAddr := ""
	{
		conn, err := grpc.Dial(gFrontNotifyMngAddr, grpc.WithInsecure())
		if err != nil {
			log.Printf("connect frontMng failed. [%+v]", err)
		}
		client := pb.NewFrontNotifySvrMngClient(conn)
		//获取frontSvr地址
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		rspAdr, err := client.QueryMyFrontNotifySvr(ctx, &pb.CClientAuthInfo{UserId:userId})
		cancel()
		if err != nil{
			return nil, nil, err
		}
		frontAddr = rspAdr.Addr
		//
		_ = conn.Close()
	}
	//连接front
	conn, err := grpc.Dial(frontAddr, grpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}
	frontClient := pb.NewFrontNotifySvrClient(conn)

	return conn, frontClient, nil
}
func connFrontSvr(userId int64) (*grpc.ClientConn, pb.FrontSvrClient, error){
	//连接frontMng获取frontSvr地址
	frontAddr := ""
	{
		conn, err := grpc.Dial(gFrontMngAddr, grpc.WithInsecure())
		if err != nil {
			return nil, nil, err
		}
		client := pb.NewFrontSvrMngClient(conn)
		//获取frontSvr地址
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		rspAdr, err := client.QueryMyFrontSvr(ctx, &pb.CClientAuthInfo{UserId:userId})
		cancel()
		if err != nil{
			log.Printf("QueryMyFrontSvr failed. [%+v]", err)
			return nil, nil, err
		}
		frontAddr = rspAdr.Addr
		//
		_ = conn.Close()
	}
	//连接front
	conn, err := grpc.Dial(frontAddr, grpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}
	frontClient := pb.NewFrontSvrClient(conn)

	return conn, frontClient, nil
}


