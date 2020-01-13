package cron

import (
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	NameSpace = "POD_NAMESPACE"
	Service   = "POD_SERVICE"
	LockName  = "POD_LOCK_NAME"
	PodName   = "POD_NAME"
	HostName  = "HOSTNAME"
)

type Option func(c *Cron)

// cron 负责维护 维护entry连接，调度函数
type Cron struct {
	entry     *Entry
	parser    ParseFunc
	scheduler ScheduleInterface
	election  ElectionInterface
	nextID    int64 // 下一个id
	chain     []Middleware
	stop      chan struct{}
	wake      chan struct{}
	running   bool
	in        chan string
	noclient  bool
	config    string
	// 是否不阻塞运行
	daemon bool
	sync.Mutex
}

type ParseFunc func(string) (ParserInterface, error)

type ParserInterface interface {
	Next(time.Time) time.Time
}

type ScheduleInterface interface {
	Run(job Job)
	Schedule(job Job, name string)
}

func New(opts ...Option) *Cron {
	c := &Cron{
		entry:     &Entry{},
		parser:    Parse,
		scheduler: nil,
		election:  nil,
		chain:     nil,
		in:        make(chan string),
		stop:      make(chan struct{}),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func WithParser(parser ParseFunc) Option {
	return func(c *Cron) {
		c.parser = parser
	}
}

func WithScheduler(scheduler ScheduleInterface) Option {
	return func(c *Cron) {
		c.scheduler = scheduler
	}
}

func WithDaemon() Option {
	return func(c *Cron) {
		c.daemon = true
	}
}

func WithConfig(config string) Option {
	return func(c *Cron) {
		c.config = config
	}
}

func WithMiddleware(middleware Middleware) Option {
	return func(c *Cron) {
		c.chain = append(c.chain, middleware)
	}
}

func (c *Cron) bootstrap() {
	// init client must be first
	InitClient(c.config)

	// register single
	c.RegisterSingle()

	// init scheduler
	if c.scheduler == nil {
		c.scheduler = NewMultiScheduler(WithRemoteChan(c.in))
	}

	// init election
	if c.election == nil {
		c.election = NewElection(WithOnNewLeader(c.onNewLeader))
	}
}

// 新建一个定时任务
func (c *Cron) Add(spec string, f Job) error {
	parser, err := c.parser(spec)
	if err != nil {
		return err
	}
	id := atomic.AddInt64(&c.nextID, 1)
	c.entry.NewNode(id, spec, parser, f)
	c.tryWake()
	return nil
}

func (c *Cron) tryWake() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
	return
}

func (c *Cron) onNewLeader(identify string) {
	log.Print("new leader....")
	log.Print(identify, c.election.Identify())
	c.Stop()
	c.Lock()
	c.running = true
	c.Unlock()
	if c.election.Identify() == identify {
		// run as leader
		c.leader()
		return
	}
	// run as follower
	c.follower()
}

func (c *Cron) leader() {
	timer := time.NewTimer(time.Second)
Next:
	if c.noJob() {
		// 没有任务，休眠，等待被唤醒
		c.sleep()
		goto Next
	}
	jobs, next := c.entry.Next()
	if next.IsZero() {
		goto Next
	}
	timer.Reset(next.Sub(time.Now()))
	for {
		select {
		case <-timer.C:
			job := jobs
			node := job
			for job != nil {
				go func(job *Node) {
					chain := Chain(c.chain...)
					c.scheduler.Schedule(chain(job.method), job.name)
				}(job)
				node = job
				job = job.link
				c.entry.resetNode(node)
			}
			goto Next
		case name := <-c.in:
			log.Print("leader receive ", name)
			job, err := c.ParseFuncName(name)
			if err != nil {
				log.Printf("parse func name for %s failed, error: %s", name, err.Error())
				continue
			}
			go func() {
				chain := Chain(c.chain...)
				c.scheduler.Run(chain(job))
			}()
		case <-c.stop:
			timer.Stop()
			return
		}
	}
}

func (c *Cron) follower() {
	log.Print("becoming follower")
	for {
		select {
		case name := <-c.in:
			log.Print("follower receive ", name)
			job, err := c.ParseFuncName(name)
			if err != nil {
				log.Printf("parse func name for %s failed, error: %s", name, err.Error())
				continue
			}
			go func() {
				chain := Chain(c.chain...)
				c.scheduler.Run(chain(job))
			}()
		case <-c.stop:
			log.Print("receiving stop")
			return
		}
	}
}

func (c *Cron) noJob() bool {
	return c.entry.head == nil
}

func (c *Cron) ParseFuncName(name string) (Job, error) {
	job, err := c.entry.getJobByName(name)
	if err != nil {
		return nil, err
	}
	return job, nil
}

// 删除一个定时器
func (c *Cron) Remove(id int64) error {
	c.entry.deleteNode(id)
	return nil
}

func (c *Cron) Run() {
	// then to register leader follower func
	// start leader election
	if c.running {
		return
	}
	c.bootstrap()
	if c.daemon {
		go c.election.Run()
	} else {
		c.election.Run()
	}
}

func (c *Cron) sleep() {
	<-c.wake
}

func (c *Cron) Stop() {
	c.Lock()
	defer c.Unlock()
	if c.running {
		log.Print("stopping cron job...")
		c.stop <- struct{}{}
	}
	c.running = false
}

func (c *Cron) RegisterSingle() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		log.Print("receive signal")
		c.election.Cancel()
		c.Stop()
	}()
}

func GetIdentify() (identify string) {
	if identify = os.Getenv(PodName); identify != "" {
		return identify
	}
	if identify = os.Getenv(HostName); identify != "" {
		return identify
	}
	rand.Seed(time.Now().UnixNano())
	return strconv.Itoa(rand.Intn(10000))
}

func GetNamespace() string {
	namespace := os.Getenv(NameSpace)
	if namespace == "" {
		log.Fatalf("please set env for %s", NameSpace)
	}
	return namespace
}

func GetService() string {
	service := os.Getenv(Service)
	if service == "" {
		log.Fatalf("please set env for %s", Service)
	}
	return service
}

func GetLockName() string {
	lock := os.Getenv(LockName)
	if lock == "" {
		log.Fatalf("please set env for %s", LockName)
	}
	return lock
}
