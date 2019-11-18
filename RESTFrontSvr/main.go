/*
@Author : Ryan.wuxiaoyong
*/

package main

import (
	"WeiBo/common/rlog"
	"context"
	"fmt"
	//"github.com/kataras/iris/sessions"
	"google.golang.org/grpc"
	"strconv"
	"time"

	//"bytes"
	//"context"
	"encoding/json"
	//"errors"
	//"google.golang.org/grpc"
	"io/ioutil"
	"os"
	"log"
	//"time"
	"github.com/kataras/iris"
	"github.com/kataras/iris/middleware/recover"

	pb "WeiBo/common/protobuf"
)

//配置文件格式
type configType struct {
	MyName string `json: "myName"`
	RlogSvrAddr string `json: "rlogSvrAddr"`
	ListenAddr string  `json: "listenAddr"`
	FrontSvrMngAddr string `json: "frontSvrMngAddr"`
}
//配置文件数据对象
var gConfig = &configType{}

func main(){
	if len(os.Args)!=2 {
		log.Fatalf("xxx configPath")
	}
	confPath := os.Args[1]

	//
	confBytes, err := ioutil.ReadFile(confPath)
	if err != nil {
		log.Fatalf("Read config file failed.[%s][%+v]", confPath, err)
	}
	//解析
	err = json.Unmarshal(confBytes, gConfig)
	if err != nil {
		log.Fatalf("Read config file failed.[%s][%+v]", confPath, err)
	}

	rlog.Init(gConfig.MyName, gConfig.RlogSvrAddr)

	app := iris.New()
	app.Use(recover.New())
	app.Get("/", func(ctx iris.Context){
		_, err = ctx.HTML("<h1>Welcome</h1>" +
			`<h3>curl -H "Content-Type:application/json" -X GET http://127.0.0.1:23401/pull/111?lastMsgId=0</h3>
			<h3>curl -H "Content-Type:application/json" -X POST -d '{"followId": 222}' http://127.0.0.1:23401/follow/111</h3>
			<h3>curl -H "Content-Type:application/json" -X POST -d '{"unFollowId": 222}' http://127.0.0.1:23401/unfollow/111</h3>
			<h3>curl -H "Content-Type:application/json" -X POST -d '{"text": "hello", "imgUrls": [], "VideoUrl": ""}' http://127.0.0.1:23401/post/222</h3>`)
		if err != nil {
			rlog.Printf("ctx.HTML failed. err=[%+v]", err)
		}
	})

	// http://xxxx:xxx/pull/123/?lastMsgId=xxx
	app.Get("/pull/{userId:int}", pullHandler)
	app.Post("/post/{userId:int}", postHandler)
	app.Post("/follow/{userId:int}", followHandler)
	app.Post("/unfollow/{userId:int}", unFollowHandler)

	rlog.Printf("app.Run")
	err =app.Run(iris.Addr(gConfig.ListenAddr), iris.WithoutServerError(iris.ErrServerClosed))
	rlog.Fatalf("Iris app.Run failed. err=[%v]", err)
}

func pullHandler(ctx iris.Context){
	userId, err := ctx.Params().GetInt64("userId")
	if err != nil{
		_, _ = ctx.JSON(iris.Map{"err": "bad userId"})
		return
	}

	lastMsgIdStr := ctx.FormValue("lastMsgId")
	lastMsgId, err := strconv.Atoi(lastMsgIdStr)
	if err != nil{
		lastMsgId = 0
	}

	msgArr, err := pull(userId, int64(lastMsgId))
	if err != nil{
		rlog.Printf("pull failed. err=[%+v]", err)
		_, _ = ctx.JSON(iris.Map{"err": "pull failed"})
		return
	}

	_, err = ctx.JSON(msgArr)
	if err != nil {
		rlog.Printf("ctx.JSON failed. err=[%+v]", err)
		return
	}
}
func postHandler(ctx iris.Context){
	userId, err := ctx.Params().GetInt64("userId")
	if err != nil{
		_, _ = ctx.JSON(iris.Map{"err": "bad userId"})
		return
	}

	type postData struct {
		Text string `json:"text"`
		ImgUrls []string `json:"imgUrls"`
		VideoUrl string `json:"videoUrl"`
	}
	data := postData{}
	err = ctx.ReadJSON(&data)
	if err!=nil{
		_, _ = ctx.JSON(iris.Map{"err": "bad param"})
		return
	}

	msgId, err := post(userId, data.Text, data.ImgUrls, data.VideoUrl)
	if err != nil {
		rlog.Printf("ctx.HTML failed. err=[%+v]", err)
		_, err = ctx.JSON(iris.Map{"err": "post failed"})
		if err != nil {
			rlog.Printf("ctx.JSON failed. err=[%+v]", err)
			return
		}
	}

	_, err = ctx.JSON(iris.Map{"msgId": msgId})
	if err != nil {
		rlog.Printf("ctx.JSON failed. err=[%+v]", err)
		return
	}
}
func followHandler(ctx iris.Context){
	userId, err := ctx.Params().GetInt64("userId")
	if err != nil{
		_, _ = ctx.JSON(iris.Map{"err": "bad userId"})
		return
	}

	type followData struct {
		FollowId int64 `json:"followId"`
	}
	data := followData{}
	err = ctx.ReadJSON(&data)
	if err!=nil{
		rlog.Printf("ReadJSON failed. err=[%v]", err)
		return
	}


	err = follow(userId, data.FollowId)
	if err != nil {
		rlog.Printf("ctx.HTML failed. err=[%+v]", err)
		_, err = ctx.JSON(iris.Map{"err": "follow failed"})
		if err != nil {
			rlog.Printf("ctx.JSON failed. err=[%+v]", err)
			return
		}
	}

	_, err = ctx.JSON(iris.Map{})
	if err != nil {
		rlog.Printf("ctx.JSON failed. err=[%+v]", err)
		return
	}
}
func unFollowHandler(ctx iris.Context){
	userId, err := ctx.Params().GetInt64("userId")
	if err != nil{
		_, _ = ctx.JSON(iris.Map{"err": "bad userId"})
		return
	}

	type unFollowData struct {
		UnFollowId int64 `json:"unFollowId"`
	}
	data := unFollowData{}
	err = ctx.ReadJSON(&data)
	if err!=nil{
		rlog.Printf("ReadJSON failed. err=[%v]", err)
		return
	}


	err = unfollow(userId, data.UnFollowId)
	if err != nil {
		rlog.Printf("ctx.HTML failed. err=[%+v]", err)
		_, err = ctx.JSON(iris.Map{"err": "unfollow failed"})
		if err != nil {
			rlog.Printf("ctx.JSON failed. err=[%+v]", err)
			return
		}
	}

	_, err = ctx.JSON(iris.Map{})
	if err != nil {
		rlog.Printf("ctx.JSON failed. err=[%+v]", err)
		return
	}
}

func getFrontSvrAddr(userId int64) (string, error){
	frontAddr := ""

	conn, err := grpc.Dial(gConfig.FrontSvrMngAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("connect frontMng failed. [%+v]", err)
	}
	defer conn.Close()

	client := pb.NewFrontSvrMngClient(conn)
	//获取frontSvr地址
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	rspAdr, err := client.QueryMyFrontSvr(ctx, &pb.CClientAuthInfo{UserId:userId})
	cancel()
	if err != nil{
		log.Printf("QueryMyFrontSvr failed. [%+v]", err)
		return "", err
	}
	frontAddr = rspAdr.Addr
	//

	return frontAddr, nil
}

type MsgData struct {
	MsgId  int64
	UserId int64
	//
	Text                 string
	ImgUrlArr            []string
	VideoUrl             string
}
func pull(userId int64, lastMsgId int64) ([]*MsgData, error){
	//连接frontMng获取frontSvr地址
	frontAddr, err := getFrontSvrAddr(userId)
	if err != nil{
		return []*MsgData{}, err
	}

	//连接front
	conn, err := grpc.Dial(frontAddr, grpc.WithInsecure())
	if err != nil {
		log.Printf("connect frontSvr failed [%+v]", err)
	}
	defer conn.Close()
	frontClient := pb.NewFrontSvrClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	rsp, err := frontClient.Pull(ctx, &pb.CPullReq{UserId:userId, LastMsgId:lastMsgId})
	cancel()
	if err != nil{
		fmt.Printf("pull failed. [%+v]\n", err)
		return []*MsgData{}, err
	}
	//fmt.Printf("rsp:[%+v]\n", rsp)

	var msgArr []*MsgData
	for _, msg := range rsp.MsgArr{
		m := MsgData{MsgId: msg.MsgId, UserId: msg.UserId, Text: msg.Text, ImgUrlArr: msg.ImgUrlArr, VideoUrl: msg.VideoUrl}
		msgArr = append(msgArr, &m)
	}

	return msgArr, nil
}

func post(userId int64, text string, imgUrlArr []string, videoUrl string) (int64, error){
	//连接frontMng获取frontSvr地址
	frontAddr, err := getFrontSvrAddr(userId)
	if err != nil{
		return 0, err
	}

	//连接front
	conn, err := grpc.Dial(frontAddr, grpc.WithInsecure())
	if err != nil {
		log.Printf("connect frontSvr failed [%+v]", err)
	}
	defer conn.Close()
	frontClient := pb.NewFrontSvrClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	msg := pb.MsgData{MsgId:0, UserId:userId, Text: text, ImgUrlArr:imgUrlArr, VideoUrl:videoUrl}
	rsp, err := frontClient.Post(ctx, &pb.CPostReq{UserId:userId, Msg:&msg})
	cancel()
	if err != nil{
		fmt.Printf("pull failed. [%+v]\n", err)
		return 0, err
	}
	//fmt.Printf("rsp:[%+v]\n", rsp)

	return rsp.MsgId, nil
}

func follow(userId int64, followId int64) error {
	//连接frontMng获取frontSvr地址
	frontAddr, err := getFrontSvrAddr(userId)
	if err != nil{
		return err
	}

	//连接front
	conn, err := grpc.Dial(frontAddr, grpc.WithInsecure())
	if err != nil {
		log.Printf("connect frontSvr failed [%+v]", err)
	}
	defer conn.Close()
	frontClient := pb.NewFrontSvrClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = frontClient.Follow(ctx, &pb.CFollowReq{UserId:userId, FollowedUserId:followId})
	cancel()
	if err != nil{
		fmt.Printf("Follow failed. [%+v]\n", err)
		return err
	}
	//fmt.Printf("rsp:[%+v]\n", rsp)

	return nil
}
func unfollow(userId int64, unfollowId int64) error{
	//连接frontMng获取frontSvr地址
	frontAddr, err := getFrontSvrAddr(userId)
	if err != nil{
		return err
	}

	//连接front
	conn, err := grpc.Dial(frontAddr, grpc.WithInsecure())
	if err != nil {
		log.Printf("connect frontSvr failed [%+v]", err)
	}
	defer conn.Close()
	frontClient := pb.NewFrontSvrClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = frontClient.UnFollow(ctx, &pb.CUnFollowReq{UserId:userId, UnFollowedUserId:unfollowId})
	cancel()
	if err != nil{
		fmt.Printf("UnFollow failed. [%+v]\n", err)
		return err
	}
	//fmt.Printf("rsp:[%+v]\n", rsp)

	return nil
}

