package events

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"time"
	"webook/interactive/repository"
	"webook/pkg/logger"
	"webook/pkg/saramax"
)

const TopicReadEvent = "article_read"

type InteractiveReadEventConsumer struct {
	repo   repository.InteractiveRepository
	client sarama.Client
	l      logger.LoggerV1
	vector *prometheus.SummaryVec
}

func NewInteractiveReadEventConsumer(repo repository.InteractiveRepository, client sarama.Client, l logger.LoggerV1) *InteractiveReadEventConsumer {
	vector := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: "harmonic",
		Subsystem: "webook",
		Name:      "kafka" + "_consume_time",
		Help:      "统计 kafka 消费数据的时间",
		ConstLabels: map[string]string{
			"instance_id": "my_instance",
		},
		Objectives: map[float64]float64{
			0.5:   0.01,
			0.75:  0.01,
			0.9:   0.01,
			0.99:  0.001,
			0.999: 0.0001,
		},
	}, []string{"topic"})
	prometheus.MustRegister(vector)
	return &InteractiveReadEventConsumer{repo: repo,
		client: client,
		l:      l,
		vector: vector}
}

func (i *InteractiveReadEventConsumer) Start() error {
	cg, err := sarama.NewConsumerGroupFromClient("interactive", i.client)
	if err != nil {
		return err
	}

	go func() {
		er := cg.Consume(context.Background(), []string{TopicReadEvent}, saramax.NewHandler[ReadEvent](i.Consume, i.l))
		if er != nil {
			i.l.Error("退出消费", logger.Error(er))
		}
	}()
	return err
}

func (i *InteractiveReadEventConsumer) Consume(msg *sarama.ConsumerMessage, event ReadEvent) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	start := time.Now()
	defer func() {
		duration := time.Since(start).Milliseconds()
		i.vector.WithLabelValues(msg.Topic).Observe(float64(duration))
	}()
	return i.repo.IncrReadCnt(ctx, "article", event.Aid)
}

type ReadEvent struct {
	Aid int64
	Uid int64
}
