/*
@Author : Ryan.wuxiaoyong
*/

package common

import "fmt"

//客户端心跳间隔
const ClientHeartBeatIntervalSec = 30
//客户端心跳超时时长
const ClientMaxIdleSec = 70

//把客户端请求在服务集群内部的命令字
const (
	CmdPull = 1
	CmdPost = 2
	CmdFollow = 3
	CmdUnFollow = 4
	CmdLike = 5
	CmdOnline = 6
	CmdOffline = 7
)
//内部命令定义
type ReqCmdT struct {
	//req编号，本机唯一
	ReqId int64

	Cmd int
	ReqMsg interface{} //req数据体指针，内容以及格式由命令收发方共同协商定义
}
type RspCmdT struct {
	//req编号，本机唯一
	ReqId int64

	RspMsg interface{} //rsp数据体的指针，内容以及格式由命令收发方共同协商定义
}

type ReqOnline struct {
	UserId int64
	FrontNotifySvrId int32
}
type ReqOffline struct {
	UserId int64
}

//一次pull请求最多拉取多少条msg
const MaxMsgCountInPull = 15


//DB//////////////////////

//用户V级定义
const (
	UserLevelNormal = 0
	UserLevelSmall = 1
	UserLevelMid = 2
	UserLevelBig = 3
	UserLevelSuper = 4
)

//lastMsgId表
type DBLastMsgId struct {
	LastMsgId int64
}
//usrMsgId表
type DBUserMsgId struct {
	UserId int64
	MsgId int64
}
//msgContent表
type DBMsgContent struct{
	MsgId int64
	Text string
	VideoUrl string
	ImgUrlArr []string
}
//follow表
type DBFollow struct{
	UserId int64
	FollowId int64
	FollowTime int64
}
//用户V级表
type DBUserLevel struct{
	UserId int64
	Level int
	FollowerCount int

	//
	InTrans bool
	TransBeginTime int64
}
//用户followed表
type DBFollowed struct{
	UserId int64
	FollowedId int64
	FollowTime int64
}
//用户升级迁移临时followed表，记录迁移过程中发生的followed关系
type DBTransFollowed struct{
	UserId int64
	FollowedId int64
	FollowTime int64
}
//用户升级转移临时TransUnFollowed表，记录迁移过程中发生的unfollowed关系
type DBTransUnFollowed struct{
	UserId int64
	FollowedId int64
}

//大V followed表名
func FollowedColNameOfVIP(userId int64) string{
	return fmt.Sprintf("FollowedVIP_%d", userId)
}
//DB end //////////////////////