package main

import (
	"context"
	"due_crash/common/message"
	"github.com/dobyte/due/locate/redis/v2"
	"github.com/dobyte/due/registry/etcd/v2"
	"github.com/dobyte/due/v2"
	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/cluster/node"
	"github.com/dobyte/due/v2/encoding/json"
	"github.com/dobyte/due/v2/log"
	"github.com/dobyte/due/v2/session"
	"sync"
	"time"
)

var proxy *node.Proxy

func main() {
	// 创建容器
	container := due.NewContainer()
	// 创建用户定位器
	locator := redis.NewLocator()
	// 创建服务发现
	registry := etcd.NewRegistry()
	// 创建节点组件
	component := node.NewNode(
		node.WithName("node"),
		node.WithLocator(locator),
		node.WithRegistry(registry),
		node.WithCodec(json.DefaultCodec),
	)
	// 注册监听
	initListen(component.Proxy())
	// 保存proxy对象
	proxy = component.Proxy()
	// 添加节点组件
	container.Add(component)
	// 启动容器
	container.Serve()
}

func initListen(proxy *node.Proxy) {
	proxy.AddRouteHandler(message.RouteAuth, false, auth)
	proxy.AddRouteHandler(message.RouteTest, false, handler)
	proxy.AddRouteHandler(message.RouteCrash, false, handlerCrash)
}

var (
	users    []int64
	rwLocker sync.RWMutex
	once     sync.Once
)

func auth(ctx node.Context) {
	var res = &message.Auth{}
	ctx.Defer(func() {
		err := ctx.Response(res)
		if err != nil {
			log.Error(err)
		}
	})
	var req = &message.Auth{}
	if err := ctx.Parse(req); err != nil {
		log.Errorf("parse request message failed: %v", err)
		return
	}
	err := ctx.BindGate(req.Uid)
	if err != nil {
		log.Errorf("bind gate err: %v", err)
	}
	rwLocker.Lock()
	users = append(users, req.Uid)
	rwLocker.Unlock()
	log.Infof("auth: %v", req.Uid)
	res = &message.Auth{Uid: req.Uid}
}

func handler(ctx node.Context) {
	var res = &message.TestMsg{}
	ctx.Defer(func() {
		err := ctx.Response(res)
		if err != nil {
			log.Error(err)
		}
	})
	var req = &message.TestMsg{}
	if err := ctx.Parse(req); err != nil {
		log.Errorf("parse request message failed: %v", err)
		return
	}
	log.Infof("receive test message: %v", req.Msg)
	res.Msg = req.Msg
	err := proxy.Multicast(ctx.Context(), &cluster.MulticastArgs{
		GID:     "",
		Kind:    session.User,
		Targets: []int64{ctx.UID()},
		Message: &cluster.Message{
			Seq:   0,
			Route: message.RouteMulticast,
			Data:  &message.MulticastMsg{Msg: "multicast normal"},
		},
	})
	if err != nil {
		log.Errorf("multicast failed: %v", err)
	}
	once.Do(func() {
		go func() {
			t := time.NewTicker(time.Millisecond * 50)
			for {
				select {
				case <-t.C:
					crashLoop()
				}
			}
		}()
	})
}

func crashLoop() {
	rwLocker.RLock()
	defer rwLocker.RUnlock()
	err := proxy.Multicast(context.Background(), &cluster.MulticastArgs{
		GID:     "",
		Kind:    session.User,
		Targets: users,
		Message: &cluster.Message{
			Route: message.RouteMulticast,
			Data:  &message.MulticastMsg{Msg: "multicast msg"},
		},
	})
	if err != nil {
		log.Warnf("multicast failed: %v", err)
	}
}

func handlerCrash(ctx node.Context) {
	var res = &message.TestMsg{}
	ctx.Defer(func() {
		err := ctx.Response(res)
		if err != nil {
			log.Error(err)
		}
	})
	var req = &message.TestMsg{}
	if err := ctx.Parse(req); err != nil {
		log.Errorf("parse request message failed: %v", err)
		return
	}
	res = &message.TestMsg{Msg: req.Msg}
	log.Infof("receive crash message: %v", req.Msg)

}
