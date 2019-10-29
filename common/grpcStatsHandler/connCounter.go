/*
@Author : Ryan.wuxiaoyong
*/

package grpcStatsHandler

import (
	"WeiBo/common/rlog"
	"context"
	"google.golang.org/grpc/stats"
	"sync"
)


type ConnCounterHandler struct{
	sync.Mutex
	connCounter int

	//如果非nil，则在连接断开时会以连接id为参数调用此回调
	ConnEndCallBack func(connId int64)
}

func (h *ConnCounterHandler)GetConnCount() int{
	return h.connCounter
}

func (h *ConnCounterHandler)GetConnId(ctx context.Context) (int64, bool){
	id, ok := ctx.Value(connCtxKey{}).(int64)
	return id, ok
}

func (h *ConnCounterHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return context.WithValue(ctx, connCtxKey{}, connIdGen.getOneId())
}

func (h *ConnCounterHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return ctx
}

func (h *ConnCounterHandler) HandleConn(ctx context.Context, s stats.ConnStats) {
	switch s.(type) {
	case *stats.ConnBegin:
		h.Lock()
		h.connCounter++
		h.Unlock()
	case *stats.ConnEnd:
		h.Lock()
		h.connCounter--
		if h.connCounter < 0{
			rlog.Printf("shit connCounter=%d", h.connCounter)
			h.connCounter = 0
		}
		h.Unlock()

		//
		connId, ok := h.GetConnId(ctx)
		if ok {
			if h.ConnEndCallBack != nil{
				h.ConnEndCallBack(connId)
			}
		}

	default:
		rlog.Printf("illegal ConnStats type.[%+v]", s)
	}
}

func (h *ConnCounterHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
}

//conn编号，保证本机唯一
type connIdGenT struct {
	sync.Mutex

	lastConnId int64
}
func (g *connIdGenT)getOneId() int64{
	g.Lock()
	defer g.Unlock()
	g.lastConnId++
	return g.lastConnId
}
//
var connIdGen = connIdGenT{sync.Mutex{}, 0}

type connCtxKey struct {}


//
//type ConnCounterHandler struct{
//	sync.Mutex
//
//	connCounter int
//}
//
//func (h *ConnCounterHandler)GetConnCount() int{
//	h.Lock()
//	defer h.Unlock()
//
//	return h.connCounter
//}
//
//
//func (h *ConnCounterHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
//	return ctx
//}
//
//func (h *ConnCounterHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
//	return ctx
//}
//
//func (h *ConnCounterHandler) HandleConn(ctx context.Context, s stats.ConnStats) {
//
//	h.Lock()
//	defer h.Unlock()
//
//	switch s.(type) {
//	case *stats.ConnBegin:
//		h.connCounter++
//	case *stats.ConnEnd:
//		h.connCounter--
//		if h.connCounter < 0{
//			rlog.Printf("shit connCounter=%d", h.connCounter)
//			h.connCounter = 0
//		}
//	default:
//		rlog.Printf("illegal ConnStats type\n")
//	}
//}
//
//func (h *ConnCounterHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
//}
