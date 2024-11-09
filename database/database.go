package database

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"golang.org/x/exp/rand"
)

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

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

type SettingNotification struct {
	Schema      string
	TableName   string
	Queue       string
	MessageType string
	Contract    string
	ServiceName string
	EventName   string
}

type DatabaseInterface interface {
	SetSetting(db *sql.DB, settings SettingNotification)
	cleanup() error
	setQueue(queue string) (bool, error)
	setMessageType() (bool, error)
	setContract() (bool, error)
	setService() (bool, error)
	setEvent() (bool, error)
	setTrigger() error
	OnNotificationEvent(lambda func(changes string))
}

var seededRand *rand.Rand = rand.New(
	rand.NewSource(uint64(time.Now().UnixNano())))

func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func String(length int) string {
	return StringWithCharset(length, charset)
}

func (s *SqlNotificationService) SetSetting(db *sql.DB, settings SettingNotification) {
	// Set database connection
	s.db = db

	// Define table name for watch notification
	s.schema = settings.Schema
	s.tableName = settings.TableName
	s.queue = settings.Queue
	s.messageType = settings.MessageType
	s.contract = settings.Contract
	s.serviceName = settings.ServiceName
	s.eventName = settings.EventName
	s.triggerName = ("tr_sbg_" + s.tableName + "-" + String(8))

	if err := s.cleanup(); err != nil {
		log.Fatal("Se ha generado un error al limpiar la base de datos:", err)
	}

	// Create queue, message type, contract and service
	if _, err := s.SetQueue(); err != nil {
		log.Fatal("Se ha generado un error al crear la cola:", err)
	}
	if _, err := s.SetMessageType(); err != nil {
		log.Fatal("Se ha generado un error al crear el mensaje:", err)
	}
	if _, err := s.SetContract(); err != nil {
		log.Fatal("Se ha generado un error al crear el contrato:", err)
	}
	if _, err := s.SetService(); err != nil {
		log.Fatal("Se ha generado un error al crear el servicio:", err)
	}
	if _, err := s.SetEvent(); err != nil {
		log.Fatal("Se ha generado un error al crear el evento:", err)
	}
	if err := s.setTrigger(); err != nil {
		log.Fatal("Se ha generado un error al crear el desencadenador:", err)
	}
}

// cleanup is used to clean up the database after a service has been stopped.
// It is called automatically when the service is stopped.
// It will drop the service, contract, message type and queue.
func (s *SqlNotificationService) cleanup() error {
	// Remove triggers
	s.removeTriggers()

	// Drop objects
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
	if err != nil {
		return err
	}

	return nil
}

func (s *SqlNotificationService) SetQueue() (bool, error) {
	_, err := s.db.Exec(fmt.Sprintf("CREATE QUEUE [%s];", s.queue))
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *SqlNotificationService) SetMessageType() (bool, error) {
	_, err := s.db.Exec(fmt.Sprintf("CREATE MESSAGE TYPE [%s] VALIDATION = NONE;", s.messageType))
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *SqlNotificationService) SetContract() (bool, error) {
	_, err := s.db.Exec(fmt.Sprintf(`CREATE CONTRACT [%s] (
	[%s] SENT BY INITIATOR
	);`, s.contract, s.messageType))
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *SqlNotificationService) SetService() (bool, error) {
	_, err := s.db.Exec(fmt.Sprintf("CREATE SERVICE [%s] ON QUEUE [%s] ([%s]);", s.serviceName, s.queue, s.contract))
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *SqlNotificationService) SetEvent() (bool, error) {
	_, err := s.db.Exec(fmt.Sprintf("CREATE EVENT NOTIFICATION %s ON QUEUE [%s] FOR QUEUE_ACTIVATION TO SERVICE '%s','current database';", s.eventName, s.queue, s.serviceName))
	if err != nil {
		return false, err
	}
	return true, nil
}

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
	if err != nil {
		return err
	}
	return nil
}

func (s *SqlNotificationService) OnNotificationEvent(lambda func(changes string)) {
	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			var messageBody string
			query := "WAITFOR (RECEIVE TOP(1) CONVERT(XML, message_body) AS message_body FROM [ChangeNotificationQueue]);"
			fmt.Println("Esperando cambios...")

			error := s.db.QueryRow(query).Scan(&messageBody)
			if error != nil && error != sql.ErrNoRows {
				log.Println("Error recibiendo el mensaje:", error)
			}

			if messageBody != "" {
				fmt.Println("Mensaje recibido:", messageBody)
				lambda(messageBody)
			}
			time.Sleep(5 * time.Second) // Espera antes de la siguiente consulta
		}
	}()

	<-done
}

func (s *SqlNotificationService) removeTriggers() {

	queryText := fmt.Sprintf(`select
	t.name as table_name,
	tr.name as trigger_name
	from sys.triggers as tr
	join sys.tables t 
		on tr.parent_id = t.object_id
	where t.name = '%s'
	and tr.name like '%s'`, s.tableName, "tr_%sbg_%")

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
