package server

import (
	"context"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"

	CronService "tyloafer/cron/cron/service/cron"
)

const (
	ListenPort = "LISTEN_PORT"
)

type Server struct {
	CronService.CronServer
	CallbackChan chan string
}

func (s Server) Scheduler(ctx context.Context, r *CronService.CronReq) (
	*CronService.CronResp, error) {
	go func() {
		s.CallbackChan <- r.Name
	}()
	return &CronService.CronResp{
		Code: 0,
		Msg:  "ok",
	}, nil
}

type Scheduled struct {
	CallbackChan chan string
}

func (s *Scheduled) Start() {
	listener, err := net.Listen("tcp", GetAddress())
	if err != nil {
		panic(err)
	}
	server := grpc.NewServer()
	CronService.RegisterCronServer(server, &Server{CallbackChan: s.CallbackChan})
	log.Printf("start listening....")
	if err = server.Serve(listener); err != nil {
		log.Fatalf("failed to listen server, error: %s", err.Error())
	}
}

func (s *Scheduled) SetCallbackChan(callback chan string) {
	s.CallbackChan = callback
}

func GetAddress() string {
	port := os.Getenv(ListenPort)
	if port == "" {
		log.Fatalf("please set env for %s", ListenPort)
	}
	return ":" + port
}
