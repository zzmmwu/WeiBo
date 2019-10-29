/*
@Author : Ryan.wuxiaoyong
*/

package main

import (
	//"net"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo"
	"strconv"

	//"strconv"
	//"sync"
	"context"
	"log"
	"go.mongodb.org/mongo-driver/bson"
	"os"
	//"time"
	"WeiBo/common"
)


func main(){
	if len(os.Args)!=4 && len(os.Args)!= 2 {
		log.Fatal("xxx mongodbUrl [username password]")
	}
	dbUrl := os.Args[1]
	usrName := ""
	pass := ""
	if len(os.Args) == 4{
		usrName = os.Args[2]
		pass = os.Args[3]
	}

	mongoOpt := options.Client().ApplyURI("mongodb://" + dbUrl) //("mongodb://139.155.0.212:27017")
	if usrName != "" {
		mongoOpt = mongoOpt.SetAuth(options.Credential{Username:usrName, Password:pass})
	}
	client, err := mongo.Connect(context.TODO(), mongoOpt)
	if err != nil {
		log.Fatal(err)
	}
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Connected to mongondb://" + dbUrl)

	createUserMsgIdCol(client)
	log.Printf("createUserMsgIdCol finished")
	createMsgContentCol(client)
	log.Printf("createMsgContentCol finished")
	createFollowCol(client)
	log.Printf("createFollowCol finished")
	createUserLevelCol(client)
	log.Printf("createUserLevelCol finished")
	createFollowedCol(client)
	log.Printf("createFollowedCol finished")
	createLastMsgIdCol(client)
	log.Printf("createLastMsgIdCol finished")

	_ = client.Disconnect(context.TODO())
	os.Exit(0)
}

//usrMsgId表：
//按照userId分成100个表。
func createUserMsgIdCol(client *mongo.Client){
	dummy := common.DBUserMsgId{UserId:0, MsgId:0}
	for i:=0; i<100; i++{
		colName := "UserMsgId_"+strconv.Itoa(i)
		collection := client.Database("WeiBo").Collection(colName)
		_, err := collection.InsertOne(context.TODO(), dummy)
		if err != nil {
			log.Printf("insert into %s failed. err=%+v", colName, err)
		}
		//log.Printf("insert into %s suc.", colName)

		//建索引
		//unique := true
		usrMsgModel := mongo.IndexModel{
			Keys: bson.D{
				{"userid", int64(1)},
				{"msgid", int64(1)},
			},
			//Options: &options.IndexOptions{Unique: &unique},
		}
		_, err = collection.Indexes().CreateOne(context.TODO(), usrMsgModel)
		if err != nil{
			log.Printf("create index on %s failed. err=%+v", colName, err)
		}
	}
}

//msgContent表：
//按照msgId分成100个表。msgId建索引
func createMsgContentCol(client *mongo.Client){
	dummy := common.DBMsgContent{MsgId:0, Text:"dummy", VideoUrl:"", ImgUrlArr:[]string{}}
	for i:=0; i<100; i++{
		colName := "MsgContent_"+strconv.Itoa(i)
		collection := client.Database("WeiBo").Collection(colName)
		_, err := collection.InsertOne(context.TODO(), dummy)
		if err != nil {
			log.Printf("insert into %s failed. err=%+v", colName, err)
		}

		//建索引
		msgIdModel := mongo.IndexModel{
			Keys: bson.D{
				{"msgid", int64(1)},
			},
		}
		_, err = collection.Indexes().CreateOne(context.TODO(), msgIdModel)
		if err != nil{
			log.Printf("create index on %s failed. err=%+v", colName, err)
		}
	}
}

//follow表：
//按照id分成300个表。用户id和关注人id建复合索引
func createFollowCol(client *mongo.Client){
	dummy := common.DBFollow{UserId:0, FollowId:0, FollowTime:0}
	for i:=0; i<300; i++{
		colName := "Follow_"+strconv.Itoa(i)
		collection := client.Database("WeiBo").Collection(colName)
		_, err := collection.InsertOne(context.TODO(), dummy)
		if err != nil {
			log.Printf("insert into %s failed. err=%+v", colName, err)
		}

		//建索引
		indexModel := mongo.IndexModel{
			Keys: bson.D{
				{"userid", int64(1)},
				{"followid", int64(1)},
			},
		}
		_, err = collection.Indexes().CreateOne(context.TODO(), indexModel)
		if err != nil{
			log.Printf("create index on %s failed. err=%+v", colName, err)
		}
	}
}

//用户V级表：
//按照id进行hash分表，分20个表
//记录用户id、v级和followed表名
func createUserLevelCol(client *mongo.Client){
	dummy := common.DBUserLevel{UserId:0, Level:0, FollowerCount:0, InTrans:false, TransBeginTime:0}
	for i:=0; i<20; i++{
		colName := "UserLevel_"+strconv.Itoa(i)
		collection := client.Database("WeiBo").Collection(colName)
		_, err := collection.InsertOne(context.TODO(), dummy)
		if err != nil {
			log.Printf("insert into %s failed. err=%+v", colName, err)
		}

		//建索引
		indexModel := mongo.IndexModel{
			Keys: bson.D{
				{"userid", int64(1)},
			},
		}
		_, err = collection.Indexes().CreateOne(context.TODO(), indexModel)
		if err != nil{
			log.Printf("create index on %s failed. err=%+v", colName, err)
		}
	}
}

//用户followed表按照v级和id进行分表
//普通用户按照id分成100个表。用户id和粉丝id建复合索引。
//所有小v，按照id分成100个表。用户id和粉丝id建复合索引。
//所有中v，按照id分成100个表。用户id和粉丝id建复合索引。
//所有大v和超v，一人一张表。无需用户id。粉丝id建索引。
func createFollowedCol(client *mongo.Client){
	for lev:=0; lev<common.UserLevelSuper; lev++{
		dummy := common.DBFollowed{UserId:0, FollowedId:0, FollowTime:0}
		for i:=0; i<100; i++{
			colName := "Followed_"+ strconv.Itoa(lev)+"_"+strconv.Itoa(i)
			collection := client.Database("WeiBo").Collection(colName)
			_, err := collection.InsertOne(context.TODO(), dummy)
			if err != nil {
				log.Printf("insert into %s failed. err=%+v", colName, err)
			}

			//建索引
			indexModel := mongo.IndexModel{
				Keys: bson.D{
					{"userid", int64(1)},
					{"followedid", int64(1)},
				},
			}
			_, err = collection.Indexes().CreateOne(context.TODO(), indexModel)
			if err != nil{
				log.Printf("create index on %s failed. err=%+v", colName, err)
			}
		}
	}

}

func createLastMsgIdCol(client *mongo.Client) {
	dummy := common.DBLastMsgId{LastMsgId: 100000000}
	colName := "LastMsgId"
	collection := client.Database("WeiBo").Collection(colName)
	_, err := collection.InsertOne(context.TODO(), dummy)
	if err != nil {
		log.Printf("insert into %s failed. err=%+v", colName, err)
	}
}
