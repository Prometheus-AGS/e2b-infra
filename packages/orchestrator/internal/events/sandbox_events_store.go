package events

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/trace"
)

const (
	cacheTL = time.Hour * 24 * 30

	EventPrefix = "ev:"
	IPPrefix    = "ip:"
)

type SandboxEvent struct {
	Path string         `json:"path"`
	Body map[string]any `json:"body"`
}

func (i SandboxEvent) MarshalBinary() ([]byte, error) {
	return json.Marshal(i)
}

type sandboxEventStore struct {
	ctx         context.Context
	tracer      trace.Tracer
	redisClient redis.UniversalClient
}

type SandboxEventStore interface {
	SetSandboxIP(sandboxId string, ip string) error
	GetSandboxIP(sandboxId string) (string, error)
	DelSandboxIP(sandboxId string) error

	GetLastEvent(sandboxId string) (*SandboxEvent, error)
	GetLastNEvents(sandboxId string, n int) ([]*SandboxEvent, error)
	AddEvent(sandboxId string, SandboxEvent *SandboxEvent, expiration time.Duration) error
	DelEvent(sandboxId string) error

	Close(ctx context.Context) error
}

func NewSandboxEventStore(ctx context.Context, tracer trace.Tracer, redisClient redis.UniversalClient) SandboxEventStore {
	return &sandboxEventStore{
		ctx:         ctx,
		tracer:      tracer,
		redisClient: redisClient,
	}
}

func (c *sandboxEventStore) SetSandboxIP(sandboxId string, ip string) error {
	return c.redisClient.Set(c.ctx, IPPrefix+ip, sandboxId, cacheTL).Err()
}

func (c *sandboxEventStore) GetSandboxIP(ip string) (string, error) {
	return c.redisClient.Get(c.ctx, IPPrefix+ip).Result()
}

func (c *sandboxEventStore) DelSandboxIP(ip string) error {
	return c.redisClient.Del(c.ctx, IPPrefix+ip).Err()
}

func (c *sandboxEventStore) GetLastEvent(sandboxId string) (*SandboxEvent, error) {
	_, span := c.tracer.Start(c.ctx, "sandbox-event-get-last")
	defer span.End()

	result, err := c.redisClient.ZRevRangeWithScores(c.ctx, EventPrefix+sandboxId, 0, 0).Result()
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, redis.Nil
	}
	rawEvent := result[0].Member.(string)

	var event SandboxEvent
	err = json.Unmarshal([]byte(rawEvent), &event)
	if err != nil {
		return nil, err
	}
	return &event, nil
}

func (c *sandboxEventStore) GetLastNEvents(sandboxId string, n int) ([]*SandboxEvent, error) {
	_, span := c.tracer.Start(c.ctx, "sandbox-event-get-last-n")
	defer span.End()

	result, err := c.redisClient.ZRevRangeWithScores(c.ctx, EventPrefix+sandboxId, 0, int64(n-1)).Result()
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, redis.Nil
	}

	events := make([]*SandboxEvent, 0, len(result))
	for _, item := range result {
		rawEvent := item.Member.(string)
		var event SandboxEvent
		err = json.Unmarshal([]byte(rawEvent), &event)
		if err != nil {
			return nil, err
		}
		events = append(events, &event)
	}

	return events, nil
}

func (c *sandboxEventStore) AddEvent(sandboxId string, event *SandboxEvent, expiration time.Duration) error {
	_, span := c.tracer.Start(c.ctx, "sandbox-event-store")
	defer span.End()

	return c.redisClient.ZAdd(c.ctx, EventPrefix+sandboxId, redis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: event,
	}).Err()
}

func (c *sandboxEventStore) DelEvent(sandboxId string) error {
	_, span := c.tracer.Start(c.ctx, "sandbox-event-delete")
	defer span.End()

	return c.redisClient.Del(c.ctx, EventPrefix+sandboxId).Err()
}

func (c *sandboxEventStore) Close(ctx context.Context) error {
	return c.redisClient.Close()
}
