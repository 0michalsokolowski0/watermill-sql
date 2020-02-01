package sql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"runtime/debug"

	"github.com/pkg/errors"

	"github.com/siddontang/go-mysql/canal"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type BinlogSubscriber struct {
	canal.DummyEventHandler

	canal  *canal.Canal
	config SubscriberConfig

	db     *sql.DB
	logger watermill.LoggerAdapter

	topics map[string]bool
}

func NewBinlogSubscriber(db *sql.DB, config SubscriberConfig, logger watermill.LoggerAdapter) (*BinlogSubscriber, error) {
	config.setDefaults()
	c, err := getDefaultCanal()
	if err != nil {
		return nil, errors.Wrap(err, "could not open canal to the db")
	}

	subscriber := BinlogSubscriber{
		canal:  c,
		config: config,
		db:     db,
		logger: logger,
		topics: map[string]bool{},
	}

	coords, err := c.GetMasterPos()
	if err != nil {
		return nil, errors.Wrap(err, "could not setup master position")
	}

	go func() {
		c.SetEventHandler(&subscriber)
		c.RunFrom(coords)
	}()

	return &subscriber, nil
}

func (s *BinlogSubscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if err := validateTopicName(topic); err != nil {
		return nil, err
	}

	if s.config.InitializeSchema {
		if err := s.SubscribeInitialize(topic); err != nil {
			return nil, err
		}
	}
	tableName := s.config.SchemaAdapter.MessagesTable(topic)
	found := s.topics[tableName]
	if !found {
		s.topics[tableName] = true
	}

	return nil, nil
}

func (s *BinlogSubscriber) Close() error {
	return nil
}

func (s *BinlogSubscriber) SubscribeInitialize(topic string) error {
	return initializeSchema(
		context.Background(),
		topic,
		s.logger,
		s.db,
		s.config.SchemaAdapter,
		s.config.OffsetsAdapter,
	)
}

func (s *BinlogSubscriber) String() string {
	return "BinlogSubscriber"
}

func (s *BinlogSubscriber) OnRow(e *canal.RowsEvent) error {
	defer func() {
		if r := recover(); r != nil {
			fmt.Print(r, " ", string(debug.Stack()))
		}
	}()

	matchedTopic := s.topics[e.Table.Name]
	//zle nazwany topic
	if e.Table.Schema != "watermill" || !matchedTopic {
		return nil
	}

	// base value for canal.DeleteAction or canal.InsertAction
	var position = 0
	var step = 1

	if e.Action == canal.UpdateAction {
		position = 1
		step = 2
	}

	for currentPosition := position; currentPosition < len(e.Rows); currentPosition += step {
		switch e.Action {
		case canal.UpdateAction:
			fmt.Printf("Updated on topic: %v", e.Table.Name)
		case canal.InsertAction:
			fmt.Printf("Inserted on topic: %v", e.Table.Name)
		case canal.DeleteAction:
			fmt.Printf("Deleted on topic: %v", e.Table.Name)
		default:
			fmt.Printf("Unknown action")
		}
	}
	return nil
}

func getDefaultCanal() (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	addr := os.Getenv("WATERMILL_TEST_MYSQL_HOST")
	if addr == "" {
		addr = "localhost"
	}
	cfg.Addr = fmt.Sprintf("%s:%d", addr, 3306)
	cfg.User = "root"
	cfg.Flavor = "mysql"

	cfg.Dump.ExecutionPath = ""

	return canal.NewCanal(cfg)
}
