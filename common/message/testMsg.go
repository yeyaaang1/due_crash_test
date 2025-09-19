package message

const (
	RouteAuth = iota + 1
	RouteTest
	RouteCrash
	RouteMulticast
)

type TestMsg struct {
	Msg string
}

type MulticastMsg struct {
	Msg string
}

type Auth struct {
	Uid int64
}
