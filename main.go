package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/denisenkom/go-mssqldb"
	"githun.com/d34ckgler/sbg/database"
)

var db *sql.DB

func main() {
	var err error
	connString := "server=192.168.110.97;user id=sa;password=HT3dcwb730!$;database=ecommerce;schema=dbo"
	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error abriendo la base de datos:", err)
	}
	defer db.Close()

	ns := database.SqlNotificationService{}

	// Establecer la configuración de la base de datos
	ns.SetSetting(db, database.SettingNotification{
		Schema:      "dbo",
		TableName:   "product",
		Queue:       "ChangeNotificationQueue",
		MessageType: "OnUpdateProduct",
		Contract:    "ProductProcessingContract",
		ServiceName: "ChangeNotificationService",
		EventName:   "ChangeNotification",
	})

	// Iniciar el servicio de notificaciones
	ns.OnNotificationEvent(func(changes string) {
		fmt.Println("Cambios detectados:", changes)
	})

	// for {
	// 	receiveMessage()
	// 	time.Sleep(5 * time.Second) // Espera antes de la siguiente consulta
	// }
}

// func receiveMessage() {
// 	var messageBody string
// 	query := "WAITFOR (RECEIVE TOP(1) CONVERT(XML, message_body) AS message_body FROM [ChangeNotificationQueue]);"
// 	fmt.Println("Esperando cambios...")
// 	err := db.QueryRow(query).Scan(&messageBody)
// 	if err != nil && err != sql.ErrNoRows {
// 		log.Println("Error recibiendo el mensaje:", err)
// 		return
// 	}
// 	if messageBody != "" {
// 		fmt.Println("Mensaje recibido:", messageBody)
// 	}
// }
