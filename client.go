package cron

import (
	"log"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	client *kubernetes.Clientset
)

func NewClient(opts ...K8SClientOption) *K8SClient {
	c := &K8SClient{
		config: "",
		outer:  false,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

type K8SClientOption func(c *K8SClient)

type K8SClient struct {
	// k8s 配置文件的地址
	config string
	// 是否在k8s集群外执行，测试使用
	outer bool
}

func WithClientConfig(config string) K8SClientOption {
	return func(c *K8SClient) {
		c.config = config
		c.outer = true
	}
}

func (c *K8SClient) Init() {
	var (
		config *rest.Config
		err    error
	)
	if c.outer {
		config, err = clientcmd.BuildConfigFromFlags("",
			c.config)
	} else {
		config, err = rest.InClusterConfig()
	}
	client, err = kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}
	return
}

func InitClient(config string) {
	opts := make([]K8SClientOption, 0, 0)
	if config != "" {
		opts = append(opts, WithClientConfig(config))
	}
	c := NewClient(opts...)
	c.Init()
}

func Client() *kubernetes.Clientset {
	if client != nil {
		return client
	}
	log.Fatal("please init k8s client first")
	return nil
}
