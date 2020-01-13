package cron

import (
	"context"
	"time"

	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

var (
	defaultLeaseDuration = 15 * time.Second
	defaultRenewDeadline = 10 * time.Second
	defaultRetryPeriod   = 2 * time.Second
)

type ElectionInterface interface {
	Run()
	Cancel()
	Identify() string
}

type ElectionOption func(election *Election)

type Election struct {
	identify      string
	namespace     string
	lockName      string
	leaseDuration time.Duration
	renewDeadline time.Duration
	retryPeriod   time.Duration
	// 回调函数
	onStartedLeading func(ctx context.Context)
	onStoppedLeading func()
	onNewLeader      func(identity string)

	cancel func()
}

func NewElection(opts ...ElectionOption) *Election {
	election := &Election{
		identify:         GetIdentify(),
		namespace:        GetNamespace(),
		lockName:         GetLockName(),
		leaseDuration:    defaultLeaseDuration,
		renewDeadline:    defaultRenewDeadline,
		retryPeriod:      defaultRetryPeriod,
		onStartedLeading: defaultOnStartedLeading,
		onStoppedLeading: defaultOnStoppedLeading,
		onNewLeader:      defaultOnNewLeader,
	}
	for _, opt := range opts {
		opt(election)
	}
	return election
}

func WithOnStartedLeading(f func(ctx context.Context)) ElectionOption {
	return func(election *Election) {
		election.onStartedLeading = f
	}
}

func WithOnStoppedLeading(f func()) ElectionOption {
	return func(election *Election) {
		election.onStoppedLeading = f
	}
}

func WithOnNewLeader(f func(identity string)) ElectionOption {
	return func(election *Election) {
		election.onNewLeader = f
	}
}

func WithElectionIdentify(identity string) ElectionOption {
	return func(election *Election) {
		election.identify = identity
	}
}

func WithDefaultLeaseDuration(duration time.Duration) ElectionOption {
	return func(election *Election) {
		election.leaseDuration = duration
	}
}

func WithDefaultRenewDeadline(duration time.Duration) ElectionOption {
	return func(election *Election) {
		election.renewDeadline = duration
	}
}

func WithDefaultRetryPeriod(duration time.Duration) ElectionOption {
	return func(election *Election) {
		election.retryPeriod = duration
	}
}

func defaultOnStartedLeading(ctx context.Context) {
	return
}

func defaultOnStoppedLeading() {
	return
}

func defaultOnNewLeader(identity string) {
	return
}

func (e *Election) Run() {
	client := Client()
	ctx, cancel := context.WithCancel(context.Background())
	e.SetCancel(cancel)
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metaV1.ObjectMeta{
			Name:      e.lockName,
			Namespace: e.namespace,
		},
		Client: client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: e.identify,
		},
	}
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   e.leaseDuration,
		RenewDeadline:   e.renewDeadline,
		RetryPeriod:     e.retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: e.onStartedLeading,
			OnStoppedLeading: e.onStoppedLeading,
			OnNewLeader:      e.onNewLeader,
		},
	})
}

func (e *Election) Cancel() {
	e.cancel()
}

func (e *Election) SetCancel(cancel func()) {
	e.cancel = cancel
}

func (e *Election) Identify() string {
	return e.identify
}
