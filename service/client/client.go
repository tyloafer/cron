package client

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	CronService "github.com/tyloafer/cron/service/cron"
)

// schedule job for name to host to run
func Schedule(host string, name string) error {
	conn, err := grpc.Dial(host, grpc.WithBlock(), grpc.WithInsecure())
	if err != nil {
		panic(err)
		return err
	}
	defer conn.Close()
	client := CronService.NewCronClient(conn)
	resp, err := client.Scheduler(context.Background(), &CronService.CronReq{
		Name: name,
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", resp)
	return nil
}
