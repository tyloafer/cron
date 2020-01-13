// entry结构，提供堆排序等功能
package cron

import (
	"errors"
	"reflect"
	"runtime"
	"sync"
	"time"
)

// 二叉树节点
type Entry struct {
	head    *Node
	records sync.Map
	sync.Mutex
}

type Node struct {
	id      int64
	link    *Node           // 由相同启动时间组成job链表
	next    *Node           // 右侧节点
	spec    string          // cron 原始表达式
	parse   ParserInterface // cron 的解析结构体
	runAt   time.Time       // 下一次的启动时间，unix
	method  Job             // 运行的方法
	name    string          // 函数名
	deleted bool            // 标识是否被删除
}

// 生成一个新的entry节点
func (c *Entry) NewNode(id int64, spec string, parse ParserInterface, f Job) (node *Node) {
	name := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
	node = &Node{
		id:     id,
		link:   nil,
		next:   nil,
		spec:   spec,
		parse:  parse,
		runAt:  parse.Next(time.Now()),
		method: f,
		name:   name,
	}
	c.records.Store(id, node)
	c.records.Store(name, node)
	c.addNode(node)
	return node
}

func (c *Entry) resetNode(node *Node) {
	node.runAt = node.parse.Next(time.Now())
	node.link = nil
	node.next = nil
	c.addNode(node)
}

// 增加一个节点，并按照时间发生进行排序
func (c *Entry) addNode(node *Node) {
	// 在entry链表中插入对应的位置
	c.Lock()
	defer c.Unlock()
	if c.head == nil {
		c.head = node
		return
	}
	if c.head.runAt.After(node.runAt) {
		node.next = c.head
		c.head = node
		return
	}

	if c.head.runAt.Equal(node.runAt) {
		node.link = c.head
		node.next = c.head.next
		c.head.next = nil
		c.head = node
		return
	}

	// 首先判断头部
	entry := c.head
	prev := entry
	for entry.runAt.Unix() < node.runAt.Unix() {
		if entry.next == nil {
			entry.next = node
			return
		}
		prev = entry
		entry = entry.next
	}
	if entry.runAt.Equal(node.runAt) {
		node.next = entry.next
		node.link = entry
		entry.next = nil
		prev.next = node
	} else {
		node.next = entry
		prev.next = node
	}
}

func (c *Entry) getNode(id int64) (*Node, error) {
	node, ok := c.records.Load(id)
	if !ok {
		return nil, errors.New("node not found")
	}
	return node.(*Node), nil
}

func (c *Entry) getJobByName(name string) (Job, error) {
	node, ok := c.records.Load(name)
	if !ok {
		return nil, errors.New("node not found")
	}
	return node.(*Node).method, nil
}

func (c *Entry) deleteNode(id int64) {
	node, err := c.getNode(id)
	if err != nil {
		return
	}
	node.deleted = true
	c.records.Delete(node.id)
	c.records.Delete(node.name)
}

func (c *Entry) Next() (node *Node, next time.Time) {
	if c.head == nil {
		return nil, time.Time{}
	}
	node = c.head
	c.head = node.next
	node.next = nil
	return node, node.runAt
}
