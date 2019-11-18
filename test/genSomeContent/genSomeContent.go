/*
@Author : Ryan.wuxiaoyong
*/

package main


import (
	"bytes"
	"context"
	"errors"
	"google.golang.org/grpc"
	"net/http"
	//"strings"

	//"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"log"
	"time"

	//"time"
	//"sync/atomic"
	"encoding/json"
	"io/ioutil"
	//"sync"
	"os"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	pb "WeiBo/common/protobuf"
)

//配置文件格式
type configType struct {
	MongoDBUrl string `json: "mongoDBUrl"`
	MongoDBName string `json: "mongoDBName"`
	MsgIdGenSvrAddr string `json: "msgIdGenSvrAddr"`
	UserMsgIdColCount int		`json: "userMsgIdColCount"`
	ContentColCount int			`json: "contentColCount"`
	ESUrl string `json: "esUrl"`
	Contents []string `json: "contents"`
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

	for i := range gConfig.Contents{
		storeOneMsg(client, 1000+int64(i)%10, i)
	}

}

func storeOneMsg(client *mongo.Client, userId int64, contentIndex int){
	//先获取msgId
	msgId, err := genMsgId()
	if err != nil {
		log.Fatalf("genMsgId failed err=[%+v]", err)
		return
	}

	{
		colName := fmt.Sprintf("UserMsgId_%d", userId%int64(gConfig.UserMsgIdColCount))
		collection := client.Database(gConfig.MongoDBName).Collection(colName)
		data := bson.D{{"userid", userId}, {"msgid", msgId}}
		_, err = collection.InsertOne(context.TODO(), data)
		if err != nil{
			log.Fatalf("InsertMany failed. err=[%+v]", err)
		}
	}


	{
		colName := fmt.Sprintf("MsgContent_%d", msgId%int64(gConfig.ContentColCount))
		collection := client.Database(gConfig.MongoDBName).Collection(colName)
		data := bson.D{{"msgid", msgId}, {"Text", gConfig.Contents[contentIndex]},
			{"videourl", "v"}, {"imgurlarr", []string{"img1", "img2"}}}
		_, err := collection.InsertOne(context.TODO(), data)
		if err != nil {
			log.Fatalf("InsertMany failed. err=[%+v]", err)
		}
	}

	{
		httpClient := &http.Client{}

		updateParams := fmt.Sprintf(`{"msgid": %d, "content": "%s"}`, msgId, gConfig.Contents[contentIndex])
		log.Printf("updateParams=[%s]", updateParams)
		var jsonStr = []byte(updateParams)

		req, err := http.NewRequest("POST", "http://" + gConfig.ESUrl + "/weibo2/article2/" + fmt.Sprintf("%d", msgId) + "?pretty", bytes.NewBuffer(jsonStr) )
		if err != nil {
			// handle error
		}
		req.Header.Set("Content-Type", "application/json")

		rsp, err := httpClient.Do(req)
		if err != nil{
			log.Fatalf("req failed. err=[%+v]", err)
		}
		//log.Printf("rsp=[%v]", rsp)

		defer rsp.Body.Close()

		if rsp.StatusCode != 201{
			body, err := ioutil.ReadAll(rsp.Body)
			if err != nil {
				// handle error
			}

			fmt.Println(string(body))
			log.Fatalf("rsp=[%v]", rsp)
		}else{
			log.Printf("create suc")
		}

	}
}


//获取地址对应的链接，如果还未链接则链接
type grpcConn struct {
	Conn *grpc.ClientConn
	Client interface{}
}
func genMsgId() (int64, error){
	//服务器地址到链接的map
	addr2ConnMap := make(map[string]grpcConn)

	//允许重试一次
	for i:=0; i<2; i++{
		//获取地址对应的链接
		var connData grpcConn
		{
			var ok bool
			connData, ok = addr2ConnMap[gConfig.MsgIdGenSvrAddr]
			if !ok{
				//还未链接则链接
				conn, err := grpc.Dial(gConfig.MsgIdGenSvrAddr, grpc.WithInsecure())
				if err != nil {
					log.Printf("connect MsgIdGenSvrAddr failed. [%+v]", err)
					//连不上，视为服务down掉，忽略
					return 0, errors.New("connect MsgIdGenSvrAddr failed")
				}
				client := pb.NewMsgIdGeneratorClient(conn)
				connData = grpcConn{Conn:conn, Client:client}
				addr2ConnMap[gConfig.MsgIdGenSvrAddr] = connData
			}
		}

		//
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		rsp, err := connData.Client.(pb.MsgIdGeneratorClient).GenMsgId(ctx, &pb.GenMsgIdReq{})
		cancel()
		if err != nil {
			log.Printf("GenMsgId failed. [%+v]", err)
			_ = connData.Conn.Close()
			delete(addr2ConnMap, gConfig.MsgIdGenSvrAddr)
			//重试一次
			continue
		}
		return rsp.MsgId, nil
	}

	return 0, errors.New("GenMsgId failed")
}
