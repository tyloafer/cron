package cron

import (
	"log"
	"strconv"

	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type ConsulInterface interface {
	List() ([]string, error)
}

type K8sConsulOption func(consul *K8SConsul)

type K8SConsul struct {
	// 服务所在namespace
	namespace string
	// 服务名
	service string
	// k8s client
	clientSet *kubernetes.Clientset
}

func NewK8SConsul(opts ...K8sConsulOption) *K8SConsul {
	consul := &K8SConsul{
		namespace: GetNamespace(),
		service:   GetService(),
	}
	for _, opt := range opts {
		opt(consul)
	}
	return consul
}

func (consul *K8SConsul) List() (hosts []string, err error) {
	client := Client()
	endpoints, err := client.CoreV1().
		Endpoints(consul.namespace).Get(consul.service, metaV1.GetOptions{})
	if err != nil {
		log.Printf("get endpoints from k8s server failed, error: %s", err.Error())
		return
	}
	hosts = make([]string, 0, len(endpoints.Subsets[0].Addresses))
	for _, address := range endpoints.Subsets[0].Addresses {
		host := address.IP + ":" + strconv.Itoa(int(endpoints.Subsets[0].Ports[0].Port))
		hosts = append(hosts, host)
	}
	return
}
