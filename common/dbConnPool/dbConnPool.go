/*
@Author : Ryan.wuxiaoyong
*/

package dbConnPool

import (
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	//"log"
	"sync"
	"time"
	"WeiBo/common/rlog"
)


type MongoConnPool struct {
	sync.Mutex

	freeConnArr []*mongo.Client
	busyConnArr []*mongo.Client

	waitingChanArr []chan *mongo.Client
}
func (p *MongoConnPool)Init(size int, mongoUrl string, usrName string, pass string) error{
	p.Lock()
	defer p.Unlock()

	for i:=0; i<size; i++{
		mongoOpt := options.Client().ApplyURI("mongodb://" + mongoUrl)
		if usrName != "" {
			mongoOpt = mongoOpt.SetAuth(options.Credential{Username:usrName, Password:pass})
		}
		//超时设置
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		client, err := mongo.Connect(ctx, mongoOpt)
		cancel()
		if err != nil {
			rlog.Printf("mongodb connect failed. [%+v]", err)
			p._closeAllWithoutLock()
			return errors.New(fmt.Sprintf("mongodb connect failed. [%+v]", err))
		}
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		err = client.Ping(ctx, nil)
		cancel()
		if err != nil {
			rlog.Printf("mongodb ping failed. [%+v]", err)
			p._closeAllWithoutLock()
			return errors.New(fmt.Sprintf("mongodb connect failed. [%+v]", err))
		}

		//
		p.freeConnArr = append(p.freeConnArr, client)
	}

	return nil
}
func (p *MongoConnPool)WaitForOneConn(ctx context.Context) (*mongo.Client, error){
	p.Lock()

	var client *mongo.Client

	if len(p.freeConnArr) > 0{
		//取末尾
		client = p.freeConnArr[len(p.freeConnArr)-1]
		p.freeConnArr = p.freeConnArr[:len(p.freeConnArr)-1]

		p.busyConnArr = append(p.busyConnArr, client)

		p.Unlock()
		return client, nil
	}

	//创建自己的等待管道
	waitChn := make(chan *mongo.Client, 1)
	p.waitingChanArr = append(p.waitingChanArr, waitChn)

	//
	p.Unlock()

	//等待有人归还或超时
	select {
	case <-ctx.Done():
		p.Lock()
		defer p.Unlock()
		//
		if len(waitChn)>0{
			//还回去
			client, _ := <-waitChn
			p._returnConnWithoutLock(client)
		}
		return nil, errors.New("timeout")
	case client, ok := <-waitChn:
		if !ok{
			rlog.Printf("connpool waitChn closed. shit")
			return nil, errors.New("error")
		}
		return client, nil
	}
}
func (p *MongoConnPool)ReturnConn(client *mongo.Client){
	p.Lock()
	defer p.Unlock()

	p._returnConnWithoutLock(client)
}
func (p *MongoConnPool)_returnConnWithoutLock(client *mongo.Client){
	//有人在等，直接给他
	if len(p.waitingChanArr)>0 {
		//给第一个
		chn := p.waitingChanArr[0]
		p.waitingChanArr = p.waitingChanArr[1:]

		chn<- client
	}

	p.freeConnArr = append(p.freeConnArr, client)

	for i, c := range p.busyConnArr{
		if c == client{
			//删掉
			p.busyConnArr[i] = p.busyConnArr[len(p.busyConnArr)-1]
			p.busyConnArr = p.busyConnArr[:len(p.busyConnArr)-1]
			break
		}
	}
}
func (p *MongoConnPool)_closeAllWithoutLock(){
	for _, c := range p.freeConnArr{
		_ = c.Disconnect(context.TODO())
	}
	p.freeConnArr = []*mongo.Client{}
	//busy的就不管了
}