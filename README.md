# 延迟队列

## 简介
从事后端研发的工友（新时代农名工）都知道，延迟队列能帮助我们解决很多在业务中遇到的问题，比如订单超过30分钟自动取消、用户注册成功后，如果三天内没有登陆则进行短信提醒等等。当然这种业务场景用定时任务也可以实现，但是随着数据量的增大、产品对数据实时性要求的增加，定时任务并不能优雅的解决这些问题。
实现延迟队列有很多方式，比如利用kafka、pulsar、redis中间件，但是对于大多数的项目或者公司来说，使用kafka、pulsar这种中间件的成本是比较高的，因此此组件是基于go+redis来实现一个简单的分布式延迟队列。

## 设计目标
- 使用简洁
- 方便扩展（比如每个项目使用的redis客户端不同，工友们可随意扩展）
- 实现ack机制（自动ack、手动ack、无ack）

## 使用样例

`redis版本：zadd和zrem方法要支持批量插入、zadd要支持nx参数、支持lua脚本`

### 能力引入
```go
go get github.com/huizhang-open-source/delay_queue
```

### 启动服务
```go
func main(t *testing.T) {
	NewServer().AddTasks([]Task{
		{
			Name:        "delay-queue-1", // 延迟任务名称
			DelayTime:   10, // 延迟时间，秒
			Limit:       50, // 单个consumer每次最大的消费数量
			Consumer:    DelayQueue1Consumer{}, // 消费者处理程序
			ConsumerNum: 1, // 消费者数量
			Redis:       defaultRedis(), // 组件默认使用github.com/gomodule/redigo/redis，如不满足业务业务需要，可自行实现对应接口进行扩展
			AckType:     AckTypeAuto, // ack类型，自动、手动、禁止
			AckTimeout:  30, // 当数据取出来后，如超过此时间还未被ack，数据会被重新消费
		},
		{
			Name:        "delay-queue-2",
			DelayTime:   10,
			Limit:       2,
			Consumer:    DelayQueue2Consumer{},
			ConsumerNum: 3,
			Redis:       defaultRedis(),
			AckType:     AckTypeAuto,
			AckTimeout:  3,
		},
	}).Start()
	time.Sleep(10000 * time.Second)
}

type DelayQueue1Consumer struct {
}

func (d DelayQueue1Consumer) Deal(ctx context.Context, task Task, messages []string) error {
	fmt.Println("DelayQueue1Consumer Deal", messages)
	return nil
}

func (d DelayQueue1Consumer) Error(ctx context.Context, task Task, err *Error) {
	fmt.Println("DelayQueue1Consumer Error", *err)
}

type DelayQueue2Consumer struct {
}

func (d DelayQueue2Consumer) Deal(ctx context.Context, task Task, messages []string) error {
	fmt.Println("DelayQueue2Consumer Deal", messages)
	return nil
}

func (d DelayQueue2Consumer) Error(ctx context.Context, task Task, err *Error) {
	fmt.Println("DelayQueue2Consumer Error", *err)
}
```

### 生产消息
```go
producer := NewProducer(DefaultRedis{
    Host: "0.0.0.0",
    Port: 6379,
})
producer().PushMessage(ctx, "delay-queue-1", fmt.Sprintf("测试数据:%d=%d", time.Now().UnixNano(), i))
```
