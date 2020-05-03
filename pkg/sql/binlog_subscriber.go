package sql

import (
	"context"
	"database/sql"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/siddontang/go-mysql/schema"
	"os"
	"reflect"
	"runtime/debug"
	"strings"
	"time"

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

	topics map[string]chan *message.Message
}

func NewBinlogSubscriber(db *sql.DB, config SubscriberConfig, logger watermill.LoggerAdapter) (*BinlogSubscriber, error) {
	config.setDefaults()
	c, err := getDefaultCanal()
	if err != nil {
		return nil, errors.Wrap(err, "could not open canal to the db")
	}

	topicsToChannels := make(map[string]chan *message.Message)
	subscriber := BinlogSubscriber{
		canal:  c,
		config: config,
		db:     db,
		logger: logger,
		topics: topicsToChannels,
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

func (m *BinlogSubscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if err := validateTopicName(topic); err != nil {
		return nil, err
	}

	if m.config.InitializeSchema {
		if err := m.SubscribeInitialize(topic); err != nil {
			return nil, err
		}
	}
	tableName := m.config.SchemaAdapter.MessagesTable(topic)
	outChn, found := m.topics[tableName]
	if !found {
		outChn = make(chan *message.Message)
		m.topics[tableName] = outChn
		return outChn, nil
	}

	return outChn, nil
}

func (m *BinlogSubscriber) Close() error {
	return nil
}

func (m *BinlogSubscriber) SubscribeInitialize(topic string) error {
	return initializeSchema(
		context.Background(),
		topic,
		m.logger,
		m.db,
		m.config.SchemaAdapter,
		m.config.OffsetsAdapter,
	)
}

func (m *BinlogSubscriber) String() string {
	return "BinlogSubscriber"
}

func (m *BinlogSubscriber) OnRow(e *canal.RowsEvent) error {
	defer func() {
		if r := recover(); r != nil {
			fmt.Print(r, " ", string(debug.Stack()))
		}
	}()

	outChn, matchedTopic := m.topics[e.Table.Name]
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
		newMsg := Message{}
		err := m.GetBinLogData(&newMsg, e, currentPosition)
		if err != nil {
			panic(err)
		}
		switch e.Action {
		case canal.UpdateAction:
			oldMsg := Message{}
			m.GetBinLogData(&oldMsg, e, currentPosition-1)
			fmt.Printf("Message %s was updated in table: %s", newMsg.Uuid, e.Table.Name)
		case canal.InsertAction:
			fmt.Printf("Message %s was inserted to table: %s", newMsg.Uuid, e.Table.Name)
			outChn <- message.NewMessage(newMsg.Uuid, []byte(newMsg.Payload))
		case canal.DeleteAction:
			fmt.Printf("Message %s was removed from table: %s", newMsg.Uuid, e.Table.Name)
		default:
			fmt.Printf("Unknown action performed on table: %s", e.Table.Name)
		}
	}
	return nil
}

func (m *BinlogSubscriber) GetBinLogData(element interface{}, e *canal.RowsEvent, n int) error {
	var columnName string
	var ok bool
	v := reflect.ValueOf(element)
	s := reflect.Indirect(v)
	t := s.Type()
	num := t.NumField()
	for k := 0; k < num; k++ {
		fmt.Println(t.Field(k).Tag.Get("sql"))
		parsedTag := parseTagSetting(t.Field(k).Tag.Get("sql"))
		name := s.Field(k).Type().Name()

		if columnName, ok = parsedTag["COLUMN"]; !ok || columnName == "COLUMN" {
			continue
		}

		switch name {
		case "bool":
			s.Field(k).SetBool(m.boolHelper(e, n, columnName))
		case "int":
			s.Field(k).SetInt(m.intHelper(e, n, columnName))
		case "string":
			s.Field(k).SetString(m.stringHelper(e, n, columnName))
		case "Time":
			timeVal := m.dateTimeHelper(e, n, columnName)
			s.Field(k).Set(reflect.ValueOf(timeVal))
		case "float64":
			s.Field(k).SetFloat(m.floatHelper(e, n, columnName))
		default:
			if _, ok := parsedTag["FROMJSON"]; ok {

				newObject := reflect.New(s.Field(k).Type()).Interface()
				json := m.stringHelper(e, n, columnName)

				jsoniter.Unmarshal([]byte(json), &newObject)

				s.Field(k).Set(reflect.ValueOf(newObject).Elem().Convert(s.Field(k).Type()))
			}
		}
	}
	return nil
}

func parseTagSetting(str string) map[string]string {
	setting := map[string]string{}
	tags := strings.Split(str, ";")
	for _, value := range tags {
		v := strings.Split(value, ":")
		k := strings.TrimSpace(strings.ToUpper(v[0]))
		if len(v) >= 2 {
			setting[k] = v[1]
		} else {
			setting[k] = k
		}

	}

	return setting
}

func (m *BinlogSubscriber) dateTimeHelper(e *canal.RowsEvent, n int, columnName string) time.Time {

	columnId := m.getBinlogIdByName(e, columnName)
	if e.Table.Columns[columnId].Type != schema.TYPE_TIMESTAMP {
		panic("Not dateTime type")
	}
	t, _ := time.Parse("2006-01-02 15:04:05", e.Rows[n][columnId].(string))

	return t
}

func (m *BinlogSubscriber) intHelper(e *canal.RowsEvent, n int, columnName string) int64 {

	columnId := m.getBinlogIdByName(e, columnName)
	if e.Table.Columns[columnId].Type != schema.TYPE_NUMBER {
		return 0
	}

	switch e.Rows[n][columnId].(type) {
	case int8:
		return int64(e.Rows[n][columnId].(int8))
	case int32:
		return int64(e.Rows[n][columnId].(int32))
	case int64:
		return e.Rows[n][columnId].(int64)
	case int:
		return int64(e.Rows[n][columnId].(int))
	case uint8:
		return int64(e.Rows[n][columnId].(uint8))
	case uint16:
		return int64(e.Rows[n][columnId].(uint16))
	case uint32:
		return int64(e.Rows[n][columnId].(uint32))
	case uint64:
		return int64(e.Rows[n][columnId].(uint64))
	case uint:
		return int64(e.Rows[n][columnId].(uint))
	}
	return 0
}

func (m *BinlogSubscriber) floatHelper(e *canal.RowsEvent, n int, columnName string) float64 {

	columnId := m.getBinlogIdByName(e, columnName)
	if e.Table.Columns[columnId].Type != schema.TYPE_FLOAT {
		panic("Not float type")
	}

	switch e.Rows[n][columnId].(type) {
	case float32:
		return float64(e.Rows[n][columnId].(float32))
	case float64:
		return float64(e.Rows[n][columnId].(float64))
	}
	return float64(0)
}

func (m *BinlogSubscriber) boolHelper(e *canal.RowsEvent, n int, columnName string) bool {

	val := m.intHelper(e, n, columnName)
	if val == 1 {
		return true
	}
	return false
}

func (m *BinlogSubscriber) stringHelper(e *canal.RowsEvent, n int, columnName string) string {

	columnId := m.getBinlogIdByName(e, columnName)
	if e.Table.Columns[columnId].Type == schema.TYPE_ENUM {

		values := e.Table.Columns[columnId].EnumValues
		if len(values) == 0 {
			return ""
		}
		if e.Rows[n][columnId] == nil {
			//Если в енум лежит нуул ставим пустую строку
			return ""
		}

		return values[e.Rows[n][columnId].(int64)-1]
	}

	value := e.Rows[n][columnId]

	switch value := value.(type) {
	case []byte:
		return string(value)
	case string:
		return value
	}
	return ""
}

func (m *BinlogSubscriber) getBinlogIdByName(e *canal.RowsEvent, name string) int {
	for id, value := range e.Table.Columns {
		if value.Name == name {
			return id
		}
	}
	panic(fmt.Sprintf("There is no column %s in table %s.%s", name, e.Table.Schema, e.Table.Name))
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

type Message struct {
	Offset    int               `sql:"column:offset"`
	CreatedAt time.Time         `sql:"column:created_at"`
	Uuid      string            `sql:"column:uuid"`
	Payload   string            `sql:"column:payload"`
	Metadata  map[string]string `sql:"column:metadata;fromJson"`
}
