// Package main provides a service for SQL Server table change notifications.
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/dortizrv/sbgpkg/sbgpkg/util"
	"golang.org/x/exp/rand"
)

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// JsonResult holds the row structure for JSON serialization.
type JsonResult struct {
	Row RowStruct `json:"row"`
}

// RowStruct represents old and new values of a database row.
type RowStruct struct {
	OldValues map[string]interface{} `json:"OldValues"`
	NewValues map[string]interface{} `json:"NewValues"`
}

// SqlNotificationService manages SQL Server notifications.
type SqlNotificationService struct {
	db          *sql.DB
	schema      string
	tableName   string
	queue       string
	messageType string
	contract    string
	serviceName string
	eventName   string
	triggerName string
}

// SettingNotification holds settings for database notifications.
type SettingNotification struct {
	Schema    string
	TableName string
}

// DatabaseInterface defines methods for database notification operations.
type DatabaseInterface interface {
	SetSetting(db *sql.DB, settings SettingNotification)
	cleanup() error
	setQueue(queue string) (bool, error)
	setMessageType() (bool, error)
	setContract() (bool, error)
	setService() (bool, error)
	setEvent() (bool, error)
	setTrigger() error
	OnNotificationEvent(lambda func(v interface{}))
	UnMarshal() (map[string]interface{}, error)
	Scan(r interface{}, v interface{})
	Close()
}

var seededRand *rand.Rand = rand.New(
	rand.NewSource(uint64(time.Now().UnixNano())))

// StringWithCharset generates a random string of a given length using a specified charset.
func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

// String generates a random string of a given length using a default charset.
func String(length int) string {
	return StringWithCharset(length, charset)
}

// SetSetting configures the notification service with database settings.
func (s *SqlNotificationService) SetSetting(db *sql.DB, settings SettingNotification) {
	s.db = db
	s.schema = settings.Schema
	s.tableName = settings.TableName
	s.queue = "Change" + capitalize(s.tableName) + "Queue"
	s.messageType = "OnUpdate" + capitalize(s.tableName)
	s.contract = capitalize(s.tableName) + "ProcessingContract"
	s.serviceName = "Change" + capitalize(s.tableName) + "Service"
	s.eventName = "Change" + capitalize(s.tableName)
	s.triggerName = "tr_sbg_" + capitalize(s.tableName)

	if err := s.cleanup(); err != nil {
		log.Fatal("Error cleaning up the database:", err)
	}

	if _, err := s.SetQueue(); err != nil {
		log.Fatal("Error creating queue:", err)
	}
	if _, err := s.SetMessageType(); err != nil {
		log.Fatal("Error creating message type:", err)
	}
	if _, err := s.SetContract(); err != nil {
		log.Fatal("Error creating contract:", err)
	}
	if _, err := s.SetService(); err != nil {
		log.Fatal("Error creating service:", err)
	}
	if _, err := s.SetEvent(); err != nil {
		log.Fatal("Error creating event:", err)
	}
	if err := s.setTrigger(); err != nil {
		log.Fatal("Error creating trigger:", err)
	}
}

// cleanup removes database artifacts after stopping the service.
func (s *SqlNotificationService) cleanup() error {
	s.removeTriggers()

	queries := fmt.Sprintf(`
		IF EXISTS (SELECT * FROM sys.event_notifications WHERE name = '%s') DROP EVENT NOTIFICATION %s ON QUEUE %s;
		IF EXISTS (SELECT * FROM sys.services WHERE name = '%s') DROP SERVICE %s;
		IF EXISTS (SELECT * FROM sys.service_contracts WHERE name = '%s') DROP CONTRACT %s;
		IF EXISTS (SELECT * FROM sys.service_message_types WHERE name = '%s') DROP MESSAGE TYPE %s;
		IF EXISTS (SELECT * FROM sys.service_queues WHERE name = '%s') DROP QUEUE %s;
		`,
		s.eventName,
		s.eventName,
		s.queue,
		s.serviceName,
		s.serviceName,
		s.contract,
		s.contract,
		s.messageType,
		s.messageType,
		s.queue,
		s.queue,
	)

	_, err := s.db.Exec(queries)
	return err
}

// SetQueue creates a SQL Server queue for notifications.
func (s *SqlNotificationService) SetQueue() (bool, error) {
	_, err := s.db.Exec(fmt.Sprintf("CREATE QUEUE [%s];", s.queue))
	return err == nil, err
}

// SetMessageType creates a message type for SQL Server service broker.
func (s *SqlNotificationService) SetMessageType() (bool, error) {
	_, err := s.db.Exec(fmt.Sprintf("CREATE MESSAGE TYPE [%s] VALIDATION = NONE;", s.messageType))
	return err == nil, err
}

// SetContract creates a contract for SQL Server service broker.
func (s *SqlNotificationService) SetContract() (bool, error) {
	_, err := s.db.Exec(fmt.Sprintf(`CREATE CONTRACT [%s] (
	[%s] SENT BY INITIATOR
	);`, s.contract, s.messageType))
	return err == nil, err
}

// SetService creates a service for SQL Server service broker.
func (s *SqlNotificationService) SetService() (bool, error) {
	_, err := s.db.Exec(fmt.Sprintf("CREATE SERVICE [%s] ON QUEUE [%s] ([%s]);", s.serviceName, s.queue, s.contract))
	return err == nil, err
}

// SetEvent creates an event notification in SQL Server.
func (s *SqlNotificationService) SetEvent() (bool, error) {
	_, err := s.db.Exec(fmt.Sprintf("CREATE EVENT NOTIFICATION %s ON QUEUE [%s] FOR QUEUE_ACTIVATION TO SERVICE '%s','current database';", s.eventName, s.queue, s.serviceName))
	return err == nil, err
}

// setTrigger creates a SQL Server trigger for change notifications.
func (s *SqlNotificationService) setTrigger() error {
	_, err := s.db.Exec(fmt.Sprintf(`
CREATE TRIGGER [%s].[%s]
ON [%s].[%s]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
	DECLARE @message XML;
	SET @message = (
		SELECT
			(SELECT * FROM deleted FOR XML PATH('OldValues'), TYPE),
			(SELECT * FROM inserted FOR XML PATH('NewValues'), TYPE)
		FOR XML PATH('row'), TYPE
	);

	DECLARE @handle UNIQUEIDENTIFIER;
	BEGIN DIALOG CONVERSATION @handle
		FROM SERVICE [%s]
		TO SERVICE '%s'
		ON CONTRACT [%s]
		WITH ENCRYPTION = OFF;

	SEND ON CONVERSATION @handle
		MESSAGE TYPE [%s] (@message);
	END CONVERSATION @handle;
END;`, s.schema, s.triggerName, s.schema, s.tableName, s.serviceName, s.serviceName, s.contract, s.messageType))
	return err
}

// OnNotificationEvent processes incoming change notifications.
func (s *SqlNotificationService) OnNotificationEvent(lambda func(v RowStruct)) {
	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			var messageBody string
			query := fmt.Sprintf("WAITFOR (RECEIVE TOP(1) CONVERT(XML, message_body) AS message_body FROM [%s]), TIMEOUT 1000;", s.queue)
			row := s.db.QueryRow(query)

			if row != nil {
				if err := row.Scan(&messageBody); err != nil && err != sql.ErrNoRows {
					// Ignore errors
					// log.Println("Error receiving message:", err)
				}
			}

			if messageBody != "" {
				var resultStruct JsonResult
				jsonParsed, err := util.ReadXml(messageBody)
				if err != nil {
					log.Fatal(err)
				}
				json.Unmarshal(jsonParsed.Bytes(), &resultStruct)

				lambda(resultStruct.Row)
			}
			time.Sleep(1 * time.Second)
		}
	}()

	<-done
}

// removeTriggers removes SQL Server triggers for the specified table.
func (s *SqlNotificationService) removeTriggers() {
	queryText := fmt.Sprintf(`SELECT
	t.name AS table_name,
	tr.name AS trigger_name
	FROM sys.triggers AS tr
	JOIN sys.tables t 
		ON tr.parent_id = t.object_id
	WHERE t.name = '%s'
	AND tr.name LIKE '%s'`, s.tableName, "tr_%sbg_%")

	result, err := s.db.Query(queryText)
	if err != nil {
		log.Fatal(err)
	}

	for result.Next() {
		var table_name, trigger_name string
		if err := result.Scan(&table_name, &trigger_name); err != nil {
			log.Fatal(err)
		}

		if _, err := s.db.Query(fmt.Sprintf("DROP TRIGGER [%s]", trigger_name)); err != nil {
			log.Fatal(err)
		}
	}
}

// Scan maps data from a map to a struct using reflection.
func (s *SqlNotificationService) Scan(r interface{}, v interface{}) {
	t := reflect.ValueOf(r)
	rV := reflect.TypeOf(v)

	keys := t.MapKeys()
	validFieldCount := 0
	for _, k := range keys {
		if rV.Kind() == reflect.Ptr {
			for ind := 0; ind < rV.Elem().NumField(); ind++ {
				field := rV.Elem().Field(ind)
				tag := field.Tag.Get("json")

				if tag == k.String() {
					validFieldCount++
					rValue := reflect.ValueOf(v)
					fields := rValue.Elem().FieldByName(field.Name)

					if fields.Type() == field.Type {
						newValue := reflect.ValueOf(t.MapIndex(k).Interface())
						newType := reflect.TypeOf(t.MapIndex(k).Interface())

						if fields.Type() != newType {
							if fields.Type().String() == "int" {
								newValue, err := strconv.Atoi(newValue.String())
								if err != nil {
									log.Fatal(err)
								}
								fields.Set(reflect.ValueOf(newValue))
							} else if fields.Type().String() == "float64" {
								newValue, err := strconv.ParseFloat(newValue.String(), 64)
								if err != nil {
									log.Fatal(err)
								}
								fields.Set(reflect.ValueOf(newValue))
							}
						} else {
							fields.Set(newValue)
						}
					}
				}
			}
		}
	}
}

// Close performs cleanup when the service is stopped.
func (s *SqlNotificationService) Close() {
	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		<-sigs

		s.cleanup()
		fmt.Println("Service stopped")
		os.Exit(0)
	}()
}

// capitalize capitalizes the first letter of a string.
func capitalize(s string) string {
	if len(s) == 0 {
		return ""
	}
	return strings.ToUpper(s[:1]) + strings.ToLower(s[1:])
}
