/*
@Author : Ryan.wuxiaoyong
*/

package scaleAndBreak

import (
	"WeiBo/common/rlog"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sync"
	"time"
)

//配置文件格式
type configType struct {
	SvrAddrArr []string `json: "svrAddrArr"`
}
type configData struct {
	sync.Mutex

	SvrArr []string //服务地址列表
}
var gConfigData = configData{sync.Mutex{}, []string{}}

type AvailSvrGroup struct {
	sync.RWMutex

	SvrArr []string //可用服务地址列表
	LastChgTime time.Time //上次SvrArr发生变化的时间戳
}
var gAvailSvrGroup = AvailSvrGroup{sync.RWMutex{}, []string{}, time.Now()}

func Start(confPath string, startCheckAfterSec int, serviceCheck func(svrAddr string) error) (*AvailSvrGroup, error){
	//
	confBytes, err := ioutil.ReadFile(confPath)
	if err != nil {
		rlog.Printf("Read config file failed.[%s][%+v]", confPath, err)
		return nil, err
	}
	//解析
	config := configType{}
	err = json.Unmarshal(confBytes, &config)
	if err != nil {
		rlog.Printf("Read config file failed.[%s][%+v]", confPath, err)
		return nil, err
	}
	//
	{
		gConfigData.Lock()
		gConfigData.SvrArr = config.SvrAddrArr
		gConfigData.Unlock()
	}

	//初始化可用svr结构体
	{
		gAvailSvrGroup.Lock()

		for _, svrAddr := range config.SvrAddrArr{
			gAvailSvrGroup.SvrArr = append(gAvailSvrGroup.SvrArr, svrAddr)
		}
		gAvailSvrGroup.LastChgTime = time.Now()

		gAvailSvrGroup.Unlock()
	}

	//启动热缩扩容
	go startScaleCheck(confPath)

	//启动服务检测
	go startServiceCheck(startCheckAfterSec, serviceCheck)

	return &gAvailSvrGroup, nil
}

func startServiceCheck(startCheckAfterSec int, serviceCheck func(svrAddr string) error){

	if startCheckAfterSec > 0{
		time.Sleep(time.Duration(startCheckAfterSec)* time.Second)
	}

	for {
		time.Sleep(1 * time.Second)

		//先把config中没有的删掉
		gAvailSvrGroup.Lock()
		gConfigData.Lock()
		var addrNeedDel []string
		for _, addr := range gAvailSvrGroup.SvrArr{
			found := false
			for _, confAddr := range gConfigData.SvrArr{
				if addr == confAddr{
					found = true
					break
				}
			}
			if !found{
				//删掉
				addrNeedDel = append(addrNeedDel, addr)
			}
		}
		for _, addrDel := range addrNeedDel{
			for i, addr := range gAvailSvrGroup.SvrArr{
				if addr == addrDel{
					copy(gAvailSvrGroup.SvrArr[i:], gAvailSvrGroup.SvrArr[i+1:])
					gAvailSvrGroup.SvrArr = gAvailSvrGroup.SvrArr[:len(gAvailSvrGroup.SvrArr)-1]
					rlog.Printf("serviceRemoved(%s) . avail svr: %+v", addr, gAvailSvrGroup.SvrArr)
				}
			}
		}
		gConfigData.Unlock()
		gAvailSvrGroup.Unlock()

		//循环检测配置数据中的服务列表
		//ScaleCheck可能会并发修改数据，安全起见这里用index进行显式比较
		for index:=0; ; index++{
			svrAddr := ""
			{
				//
				gConfigData.Lock()

				if index >= len(gConfigData.SvrArr){
					gConfigData.Unlock()
					break
				}

				svrAddr = gConfigData.SvrArr[index]

				//
				gConfigData.Unlock()
			}

			//check
			err := serviceCheck(svrAddr)
			if err != nil{
				//服务有问题，熔断。从gAvailSvrGroup中删去
				gAvailSvrGroup.Lock()

				for i, addr := range gAvailSvrGroup.SvrArr{
					if addr == svrAddr{
						gAvailSvrGroup.SvrArr[i] = gAvailSvrGroup.SvrArr[len(gAvailSvrGroup.SvrArr)-1]
						gAvailSvrGroup.SvrArr = gAvailSvrGroup.SvrArr[:len(gAvailSvrGroup.SvrArr)-1]
						//
						gAvailSvrGroup.LastChgTime = time.Now()

						rlog.Warning(rlog.WarningLevelFatal, fmt.Sprintf("serviceCheck(%s) failed. server breaked. avail svr: %+v", svrAddr, gAvailSvrGroup.SvrArr), []rlog.StatPointData{})
						break
					}
				}

				gAvailSvrGroup.Unlock()
			}else{
				//检查是否在从gAvailSvrGroup中，不在则添加进去
				gAvailSvrGroup.Lock()

				found := false
				for _, addr := range gAvailSvrGroup.SvrArr{
					if addr == svrAddr{
						found = true
					}
				}
				if !found{
					gAvailSvrGroup.SvrArr = append(gAvailSvrGroup.SvrArr, svrAddr)
					//
					gAvailSvrGroup.LastChgTime = time.Now()

					rlog.Warning(rlog.WarningLevelNormal, fmt.Sprintf("serviceCheck(%s) success. server add in. avail svr %+v", svrAddr, gAvailSvrGroup.SvrArr), []rlog.StatPointData{})
				}

				gAvailSvrGroup.Unlock()
			}
		}
	}
}

func startScaleCheck(confPath string){

	for {
		time.Sleep(10 * time.Second)

		//
		confBytes, err := ioutil.ReadFile(confPath)
		if err != nil {
			rlog.Printf("Read config file failed.[%s][%+v]", confPath, err)
			continue
		}
		//解析
		config := configType{}
		err = json.Unmarshal(confBytes, &config)
		if err != nil {
			rlog.Printf("Read config file failed.[%s][%+v]", confPath, err)
			continue
		}

		//更新配置
		{
			gConfigData.Lock()
			gConfigData.SvrArr = config.SvrAddrArr
			gConfigData.Unlock()
		}
	}
}