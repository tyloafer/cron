# cron
## 简介
基于Kubernetes实现的分布式定时任务调度器

cron首先会基于k8s的leader election选举出一个leader

然后leader将解析cron表达式，然后依次调度到pod上执行

## 下载
通过 `go get` 下载 
~~~
go get github.com/tyloafer/cron@v1.0-beta.0
~~~
然后在程序中 import
~~~
import github.com/tyloafer/cron
~~~

## 使用
~~~
package main

import (
	"fmt"

	"github.com/tyloafer/cron"
)

func main() {
	c := cron.New()
    // spec => 秒 分 时 日 月 周
	err := c.Add("* * * * * *", test)
	if err != nil {
		panic(err)
	}
	c.Run()
}

func test() {
	fmt.Println("test")
}
~~~

### 添加中间件

~~~
package main

import (
	"fmt"
	"time"

	"github.com/tyloafer/cron"
)

func main() {
	c := cron.New(
		cron.WithMiddleware(middleware1),
		cron.WithMiddleware(middleware2))
	err := c.Add("* * * * * *", test)
	if err != nil {
		panic(err)
	}
	c.Run()
}

func test() {
	fmt.Println("test")
}

func middleware1(f cron.Job) cron.Job {
	return func() {
		fmt.Printf("start time: %v", time.Now())
		f()
		fmt.Printf("end time: %v", time.Now())
	}
}

func middleware2(f cron.Job) cron.Job {
	return func() {
		fmt.Printf("just test")
		f()
	}
}
~~~