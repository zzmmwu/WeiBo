/*
@Author : Ryan.wuxiaoyong
*/

package main


import (
	"context"
	//"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"log"
	"math/rand"
	"time"

	//"time"
	//"sync/atomic"
	"encoding/json"
	"io/ioutil"
	//"sync"
	"os"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//配置文件格式
type configType struct {
	MongoDBUrl string `json: "mongoDBUrl"`
	MongoDBName string `json: "mongoDBName"`
	ContentColCount int `json: "contentColCount"`
	UserMsgIdColCount int `json: "userMsgIdColCount"`
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

	mongoOpt := options.Client().ApplyURI("mongodb://" + gConfig.MongoDBUrl)
	//if usrName != "" {
	//	mongoOpt = mongoOpt.SetAuth(options.Credential{Username:usrName, Password:pass})
	//}
	//超时设置
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	client, err := mongo.Connect(ctx, mongoOpt)
	cancel()
	if err != nil {
		log.Fatalf("mongodb connect failed. [%+v]", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	err = client.Ping(ctx, nil)
	cancel()
	if err != nil {
		log.Fatalf("mongodb ping failed. [%+v]", err)
	}

	log.Printf("init")

	//普通用户0~15条微博
	createUserMsgIdX(10000000, 10000000, 0, 15)
	//10万粉以下用户15~150条微博
	createUserMsgIdX(20000000, 22000, 15, 150)
	//其他用户50~300条
	createUserMsgIdX(20022000, 310, 50, 300)

	storeUserMsgId(client)
	storeMsgContent(client)
}

type userMsgIdT struct {
	userId int64
	msgIdArr []int64
}
var gUserMsgIdArr []userMsgIdT

var gLastMsgId = int64(10000)
func createUserMsgIdX(userStartId int64, userCount int, msgMin int, msgMax int){

	for i:=0; i<userCount; i++{
		if userCount >=10 && i%(userCount/10)==0{
			log.Printf("createUserMsgIdX(userStartId=%d, userCount=%d, msgMin=%d, msgMax=%d) finished: %d/%d",
				userStartId, userCount, msgMin, msgMax, i, userCount)
		}

		userId := int64(userStartId + int64(i))
		data := userMsgIdT{userId:userId, msgIdArr:[]int64{}}
		msgCount := rand.Intn(msgMax - msgMin + 1) + msgMin
		for x:=0; x<msgCount; x++{
			gLastMsgId++
			data.msgIdArr = append(data.msgIdArr, gLastMsgId)
		}

		gUserMsgIdArr = append(gUserMsgIdArr, data)
	}

	log.Printf("createUserMsgIdX finished. lastMsgId=%d", gLastMsgId)
}

func storeUserMsgId(client *mongo.Client){
	log.Printf("storeUserMsgId start")

	for colIndex:=0; colIndex<gConfig.UserMsgIdColCount; colIndex++{
		colName := fmt.Sprintf("UserMsgId_%d", colIndex)
		log.Printf("storeUserMsgId start %s", colName)

		var dataArr []interface{}
		for i:=colIndex; i<len(gUserMsgIdArr); i+=gConfig.UserMsgIdColCount{
			userMsgData := gUserMsgIdArr[i]
			for _, msgId := range userMsgData.msgIdArr{
				data := bson.D{{"userid", userMsgData.userId}, {"msgid", msgId}}
				dataArr = append(dataArr, data)
			}
		}

		collection := client.Database(gConfig.MongoDBName).Collection(colName)
		_, err := collection.InsertMany(context.TODO(), dataArr)
		if err != nil{
			log.Fatalf("InsertMany failed. err=[%+v]", err)
		}
	}

	log.Printf("storeUserFollowInDB finished.")
}
func storeMsgContent(client *mongo.Client){
	log.Printf("storeMsgContent start")

	for colIndex:=0; colIndex<gConfig.ContentColCount; colIndex++{
		colName := fmt.Sprintf("MsgContent_%d", colIndex)
		log.Printf("storeMsgContent start %s", colName)

		var dataArr []interface{}
		for _, userMsgData := range gUserMsgIdArr{
			for _, msgId := range userMsgData.msgIdArr{
				if msgId%int64(gConfig.ContentColCount) == int64(colIndex){
					data := bson.D{{"msgid", msgId}, {"Text", fmt.Sprintf("u:%d, msg:%d", userMsgData.userId, msgId)},
						{"videourl", "v"}, {"imgurlarr", []string{"img1", "img2"}}}
					dataArr = append(dataArr, data)
				}
			}
		}

		collection := client.Database(gConfig.MongoDBName).Collection(colName)
		_, err := collection.InsertMany(context.TODO(), dataArr)
		if err != nil{
			log.Fatalf("InsertMany failed. err=[%+v]", err)
		}
	}

	log.Printf("storeMsgContent finished.")
}

