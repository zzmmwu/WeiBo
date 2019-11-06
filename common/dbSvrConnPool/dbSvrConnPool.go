/*
@Author : Ryan.wuxiaoyong
*/

package dbSvrConnPool

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"

	//"log"
	"sync"
	"WeiBo/common/rlog"
	pb "WeiBo/common/protobuf"
)


type DBSvrConnPool struct {
	sync.Mutex

	freeConnArr []*ConnData
	busyConnArr []*ConnData

	waitingChanArr []chan *ConnData
}
type ConnData struct{
	Conn *grpc.ClientConn
	Client pb.DbSvrClient
}
func (p *DBSvrConnPool)Init(size int, svrUrl string) error{
	p.Lock()
	defer p.Unlock()

	for i:=0; i<size; i++{
		conn, err := grpc.Dial(svrUrl, grpc.WithInsecure())
		if err != nil {
			return errors.New(fmt.Sprintf("dbSvr connect failed. [%+v]", err))
		}
		client := pb.NewDbSvrClient(conn)
		p.freeConnArr = append(p.freeConnArr, &ConnData{Conn:conn, Client:client})
	}

	return nil
}
func (p *DBSvrConnPool)WaitForOneConn(ctx context.Context) (*ConnData, error){
	p.Lock()

	var client *ConnData

	if len(p.freeConnArr) > 0{
		//取末尾
		client = p.freeConnArr[len(p.freeConnArr)-1]
		p.freeConnArr = p.freeConnArr[:len(p.freeConnArr)-1]

		p.busyConnArr = append(p.busyConnArr, client)

		p.Unlock()
		return client, nil
	}

	//创建自己的等待管道
	waitChn := make(chan *ConnData, 1)
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
			rlog.Printf("connPool waitChn closed. shit")
			return nil, errors.New("error")
		}
		return client, nil
	}
}
func (p *DBSvrConnPool)ReturnConn(client *ConnData){
	p.Lock()
	defer p.Unlock()

	p._returnConnWithoutLock(client)
}
func (p *DBSvrConnPool)_returnConnWithoutLock(client *ConnData){
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
func (p *DBSvrConnPool)_closeAllWithoutLock(){
	for _, c := range p.freeConnArr{
		_ = c.Conn.Close()
	}
	p.freeConnArr = []*ConnData{}
	//busy的就不管了
}