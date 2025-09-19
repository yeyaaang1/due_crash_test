package main

import (
	"due_crash/common/message"
	"github.com/dobyte/due/network/ws/v2"
	"github.com/dobyte/due/v2"
	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/cluster/client"
	"github.com/dobyte/due/v2/encoding/json"
	"github.com/dobyte/due/v2/log"
	"math/rand"
	"sync/atomic"
	"time"
)

func main() {
	// 创建容器
	container := due.NewContainer()
	// 创建客户端组件
	component := client.NewClient(
		client.WithClient(ws.NewClient()),
		client.WithCodec(json.DefaultCodec),
	)
	// 初始化监听
	initListen(component.Proxy())
	// 添加客户端组件
	container.Add(component)
	// 启动容器
	container.Serve()
}

func initListen(proxy *client.Proxy) {
	// 监听组件启动
	proxy.AddHookListener(cluster.Start, startHandler)
	// 监听连接建立
	proxy.AddEventListener(cluster.Connect, connectHandler)
	// 监听Auth消息
	proxy.AddRouteHandler(message.RouteAuth, authHandler)
	// 监听返回消息
	proxy.AddRouteHandler(message.RouteTest, respHandler)
	proxy.AddRouteHandler(message.RouteCrash, respHandler)
	// 监听广播消息
	proxy.AddRouteHandler(message.RouteMulticast, multicastHandler)
}

func multicastHandler(ctx *client.Context) {
	res := &message.MulticastMsg{}
	if err := ctx.Parse(res); err != nil {
		log.Errorf("parse request message failed: %v", err)
		return
	}
	log.Infof("receive multicast message: %v", res.Msg)
}

func respHandler(ctx *client.Context) {
	res := &message.TestMsg{}
	if err := ctx.Parse(res); err != nil {
		log.Errorf("parse request message failed: %v", err)
		return
	}
	log.Infof("receive resp message: %v", res.Msg)
}

func authHandler(ctx *client.Context) {
	go func() {
		for {
			time.Sleep(time.Second)
			doPushMessage(ctx.Conn(), message.RouteTest, &message.TestMsg{Msg: "normal test"})
		}
	}()
	doPushMessage(ctx.Conn(), message.RouteCrash, &message.TestMsg{Msg: "crash test"})
}

func startHandler(proxy *client.Proxy) {
	if _, err := proxy.Dial(); err != nil {
		log.Errorf("gate connect failed: %v", err)
		return
	}
}

// 连接建立处理器
func connectHandler(conn *client.Conn) {
	doPushMessage(conn, 1, &message.Auth{Uid: rand.Int63()})
}

var seq = atomic.Int32{}

// 推送消息
func doPushMessage(conn *client.Conn, route int32, msg any) {
	log.Infof("push message: %v", msg)
	err := conn.Push(&cluster.Message{
		Seq:   seq.Add(1),
		Route: route,
		Data:  msg,
	})
	if err != nil {
		log.Errorf("push message failed: %v", err)
	}
}
