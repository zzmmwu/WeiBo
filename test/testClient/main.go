/*
@Author : Ryan.wuxiaoyong
*/

package main


import (
	"context"

	"os"
	"strconv"
	"time"
	"fmt"
	"log"

	"google.golang.org/grpc"
	pb "WeiBo/common/protobuf"
	//"WeiBo/common/rlog"
)

func main(){


	if len(os.Args)!=4 {
		log.Fatalf("xxx frontSvrAddr frontNotifySvrAddr userId")
	}
	frontMngAddr := os.Args[1]
	frontNotifyMngAddr := os.Args[2]
	var userId int64
	userId, err := strconv.ParseInt(os.Args[3], 10, 64)
	if err != nil || userId == 0 {
		log.Fatalf("xxx serverAddr userId")
	}

	go recvNotifyRoutine(frontNotifyMngAddr, userId)
	go sendRoutine(frontMngAddr, userId)

	for{
		time.Sleep(100*time.Hour)
	}

}

func recvNotifyRoutine(mngAddr string, userId int64){
	//连接frontMng获取frontSvr地址
	frontAddr := ""
	{
		conn, err := grpc.Dial(mngAddr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("connect frontMng failed. [%+v]", err)
		}
		client := pb.NewFrontNotifySvrMngClient(conn)
		//获取frontSvr地址
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		rspAdr, err := client.QueryMyFrontNotifySvr(ctx, &pb.CClientAuthInfo{UserId:userId})
		cancel()
		if err != nil{
			log.Fatalf("QueryMyFrontNotifySvr failed. [%+v]", err)
		}
		frontAddr = rspAdr.Addr
		//
		_ = conn.Close()
	}
	//连接front
	conn, err := grpc.Dial(frontAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("connect frontSvr failed [%+v]", err)
	}
	defer conn.Close()
	frontClient := pb.NewFrontNotifySvrClient(conn)

	//心跳
	go func(){
		for{
			time.Sleep(1*time.Second)
			_, _ =frontClient.HeartBeat(context.TODO(), &pb.CHeartBeatReq{UserId:userId})
			time.Sleep(29*time.Second)
		}
	}()

	stream, err := frontClient.CreateNotifyStream(context.TODO(), &pb.CCreateNotifyStreamReq{UserId:userId})

	for{
		notify, err := stream.Recv()
		if err != nil{
			log.Fatalf("notify recv failed. err=[%+v]", err)
		}
		log.Printf("notify recved. [%+v]", notify)
	}
}

func sendRoutine(mngAddr string, userId int64){
	//连接frontMng获取frontSvr地址
	frontAddr := ""
	{
		conn, err := grpc.Dial(mngAddr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("connect frontMng failed. [%+v]", err)
		}
		client := pb.NewFrontSvrMngClient(conn)
		//获取frontSvr地址
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		rspAdr, err := client.QueryMyFrontSvr(ctx, &pb.CClientAuthInfo{UserId:userId})
		cancel()
		if err != nil{
			log.Fatalf("QueryMyFrontSvr failed. [%+v]", err)
		}
		frontAddr = rspAdr.Addr
		//
		_ = conn.Close()
	}
	//连接front
	conn, err := grpc.Dial(frontAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("connect frontSvr failed [%+v]", err)
	}
	defer conn.Close()
	frontClient := pb.NewFrontSvrClient(conn)

	//读取命令输入
	for {

		var cmd, p1, p2, p3, p4, p5 string
		fmt.Printf("enter your cmd and param: ")
		fmt.Scanln(&cmd, &p1, &p2, &p3, &p4, &p5)
		//if err != nil{
		//	fmt.Printf("bad cmd. [%+v]\n", err)
		//	continue
		//}
		switch cmd {
		case "pull":
			lastMsgId, err := strconv.ParseInt(p1, 10, 64)
			if err != nil{
				fmt.Printf("bad pull cmd . [%+v]\n", err)
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			rsp, err := frontClient.Pull(ctx, &pb.CPullReq{UserId:userId, LastMsgId:lastMsgId})
			cancel()
			if err != nil{
				fmt.Printf("pull failed. [%+v]\n", err)
				continue
			}
			fmt.Printf("rsp:[%+v]\n", rsp)
			break
		case "post":
			text := p1
			videoUrl := p2
			var imgUrlArr []string
			if p3 != ""{
				imgUrlArr = append(imgUrlArr, p3)
			}
			if p4 != ""{
				imgUrlArr = append(imgUrlArr, p4)
			}
			if p5 != ""{
				imgUrlArr = append(imgUrlArr, p5)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			rsp, err := frontClient.Post(ctx, &pb.CPostReq{UserId:userId, Msg:&pb.MsgData{MsgId:0, UserId:userId, Text:text, ImgUrlArr:imgUrlArr, VideoUrl:videoUrl}})
			cancel()
			if err != nil{
				fmt.Printf("post failed. [%+v]\n", err)
				continue
			}
			fmt.Printf("rsp:[%+v]\n", rsp)
			break
		case "follow":
			followId, err := strconv.ParseInt(p1, 10, 64)
			if err != nil{
				fmt.Printf("bad follow cmd . [%+v]\n", err)
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			rsp, err := frontClient.Follow(ctx, &pb.CFollowReq{UserId:userId, FollowedUserId:followId})
			cancel()
			if err != nil{
				fmt.Printf("follow failed. [%+v]\n", err)
				continue
			}
			fmt.Printf("rsp:[%+v]\n", rsp)
			break
		case "unfollow":
			followId, err := strconv.ParseInt(p1, 10, 64)
			if err != nil{
				fmt.Printf("bad followed cmd . [%+v]\n", err)
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			rsp, err := frontClient.UnFollow(ctx, &pb.CUnFollowReq{UserId:userId, UnFollowedUserId:followId})
			cancel()
			if err != nil{
				fmt.Printf("followed failed. [%+v]\n", err)
				continue
			}
			fmt.Printf("rsp:[%+v]\n", rsp)
			break
		default:
			fmt.Printf("bad cmd\n")
			break
		}
	}
}
