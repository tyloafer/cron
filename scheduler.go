// cron 调度算法
package cron

import (
	"log"
	"sync"
	"time"

	CronRPCClient "github.com/tyloafer/cron/service/client"
	CronRPCServer "github.com/tyloafer/cron/service/server"
)

const MaxRetry int = 3

type Scheduler struct{}

func (s *Scheduler) Run(job Job) {
	go s.run(job)
}

func (s *Scheduler) Schedule(job Job, name string) {
	go s.run(job)
}

func (s *Scheduler) run(job Job) {
	log.Print("job start", time.Now())
	job()
	log.Print("job end")
}

type MultiSchedulerOption func(ms *MultiScheduler)

type RPCSchedule func(string, string) error

type RPCScheduled interface {
	Start()
	SetCallbackChan(chan string)
}

type node struct {
	host string
	next *node
}

type link struct {
	// 第一个是空节点
	head *node
	tail *node
	sync.Mutex
}

type MultiScheduler struct {
	// 当前服务的名字
	consul ConsulInterface
	// link的head，空的节点信息，next指向第一个节点信息
	host *link
	// 判断是否在更新hosts
	async chan struct{}
	// 停止调度
	stop chan struct{}
	// refresh 是否在启动
	refresh bool

	// rpc 调度的函数实现
	schedule RPCSchedule

	// rpc 被调用的服务实现
	scheduled RPCScheduled

	cron func(name string) func()

	ready chan struct{}
	sync.Mutex
}

func NewMultiScheduler(opts ...MultiSchedulerOption) *MultiScheduler {
	scheduler := &MultiScheduler{
		consul: NewK8SConsul(),
		host: &link{
			head: &node{},
		},
		async:     make(chan struct{}),
		schedule:  CronRPCClient.Schedule,
		scheduled: &CronRPCServer.Scheduled{},
		ready:     make(chan struct{}),
	}
	for _, opt := range opts {
		opt(scheduler)
	}
	go scheduler.Refresh()
	go scheduler.Start()
	return scheduler
}

func WithConsul(consul ConsulInterface) MultiSchedulerOption {
	return func(ms *MultiScheduler) {
		ms.consul = consul
	}
}

func WithRemoteChan(c chan string) MultiSchedulerOption {
	return func(ms *MultiScheduler) {
		ms.scheduled.SetCallbackChan(c)
		go func() {
			ms.ready <- struct{}{}
		}()
	}
}

func (ms *MultiScheduler) SetRemoteChan(in chan string) {
	ms.scheduled.SetCallbackChan(in)
	ms.ready <- struct{}{}
}

func (ms *MultiScheduler) Start() {
	log.Print("scheduler start")
	<-ms.ready
	log.Print("scheduler ready")
	ms.scheduled.Start()
}

func (ms *MultiScheduler) Run(job Job) {
	job()
}

func (ms *MultiScheduler) Schedule(job Job, name string) {
	go ms.Refresh()
	for i := 0; i < MaxRetry; i++ {
		host := ms.Host()
		for host == "" {
			time.Sleep(time.Second)
			host = ms.Host()
		}
		log.Printf("schedule func %s to host %s\n", name, host)
		err := ms.schedule(host, name)
		if err == nil {
			return
		}
		extra := ""
		if i != MaxRetry-1 {
			extra = ", try it again"
		}
		log.Printf("schedule func %s to host %s failed, error: %s%s",
			name, host, err.Error(), extra)

	}
}

func (ms *MultiScheduler) Refresh() {
	timer := time.NewTimer(time.Second * 0)
	if ms.refresh {
		return
	}
	ms.refresh = true
	defer func() {
		ms.refresh = false
	}()
	for {
		select {
		case <-ms.stop:
			timer.Stop()
			return
		case <-timer.C:
			hosts, err := ms.consul.List()
			if err != nil {
				log.Printf("refresh hosts from sonsul failed, err: %s", err.Error())
				continue
			}
			ms.host.refresh(hosts)
			timer.Reset(time.Second * 3)
		}
	}
}

// Host return a host like ip:port to connect with
func (ms *MultiScheduler) Host() string {
	node := ms.host.Head()
	if node == nil {
		return ""
	}
	return node.host
}

func (link *link) refresh(hosts []string) {
	old := link.hosts()
	remove := difference(old, hosts)
	addition := difference(hosts, old)
	link.Lock()
	defer link.Unlock()
	for _, host := range remove {
		link.remove(host)
	}

	// append nodes to link tail
	for _, host := range addition {
		link.add(host)
	}
}

func (link *link) hosts() []string {
	link.Lock()
	defer link.Unlock()
	hosts := make([]string, 0, 10)
	node := link.head.next
	for node != nil {
		hosts = append(hosts, node.host)
		node = node.next
	}
	return hosts
}

// add one node with host into link tail
func (link *link) add(host string) {
	node := &node{
		host: host,
		next: nil,
	}
	if link.tail == nil {
		link.head.next = node
		link.tail = link.head.next
		return
	}
	link.tail.next = node
	link.tail = link.tail.next
}

func (link *link) remove(host string) {
	cur := link.head.next
	prev := link.head
	for cur != nil {
		if cur.host == host {
			prev.next = cur.next
			if cur.next == nil {
				link.tail = prev
			}
			return
		}
		prev = cur
		cur = cur.next
	}
}

// Head return the first node of link, and then insert the node into tail
func (link *link) Head() (node *node) {
	link.Lock()
	defer link.Unlock()
	node = link.head.next
	if node == nil || node.next == nil {
		return node
	}
	link.head.next = node.next
	node.next = nil
	link.tail.next = node
	link.tail = node
	return node
}

// difference return string array which in arr1 but not in arr2s
func difference(arr1, arr2 []string) []string {
	diff := make([]string, 0, len(arr1))
	mapping := make(map[string]int, len(arr2))
	for i, v := range arr2 {
		mapping[v] = i
	}
	for _, item := range arr1 {
		if _, ok := mapping[item]; !ok {
			diff = append(diff, item)
		}
	}
	return diff
}
