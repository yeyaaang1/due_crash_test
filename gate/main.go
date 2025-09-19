package main

import (
	"github.com/dobyte/due/locate/redis/v2"
	"github.com/dobyte/due/network/ws/v2"
	"github.com/dobyte/due/registry/etcd/v2"
	"github.com/dobyte/due/v2"
	"github.com/dobyte/due/v2/cluster/gate"
	"github.com/dobyte/due/v2/log"
	"net/http"
	_ "net/http/pprof"
)

func main() {
	// 创建容器
	container := due.NewContainer()
	// 创建服务器
	wsServer := ws.NewServer()

	// 创建用户定位器
	locator := redis.NewLocator()
	// 创建服务发现
	registry := etcd.NewRegistry()

	// 创建ws网关组件
	wsGate := gate.NewGate(
		gate.WithServer(wsServer),
		gate.WithLocator(locator),
		gate.WithRegistry(registry),
	)
	// 添加网关组件
	container.Add(wsGate)

	// 设置日志等级
	//time.AfterFunc(time.Second, func() {
	//	log.SetLogger(log.NewLogger(log.WithLevel(log.LevelInfo)))
	//})
	// 监听内存情况
	go func() {
		err := http.ListenAndServe(":6060", nil)
		log.Error(err)
	}()
	// 启动容器
	container.Serve()
}
