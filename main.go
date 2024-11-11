package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/denisenkom/go-mssqldb"
	"githun.com/d34ckgler/sbg/database"
)

var db *sql.DB

type Product struct {
	ProductID int     `json:"product_id"`
	Sku       string  `json:"sku"`
	Name      string  `json:"name"`
	Price     float64 `json:"price"`
}

type JsonResult struct {
	Row struct {
		OldValues map[string]interface{} `json:"OldValues"`
		NewValues map[string]interface{} `json:"NewValues"`
	} `json:"row"`
}

func main() {
	var err error
	connString := "server=192.168.110.97;user id=sa;password=HT3dcwb730!$;database=ecommerce;schema=dbo"
	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error opening the database:", err)
	}
	defer db.Close()

	ns := database.SqlNotificationService{}

	// Set database configuration
	ns.SetSetting(db, database.SettingNotification{
		Schema:      "dbo",
		TableName:   "product",
		Queue:       "ChangeNotificationQueue",
		MessageType: "OnUpdateProduct",
		Contract:    "ProductProcessingContract",
		ServiceName: "ChangeNotificationService",
		EventName:   "ChangeNotification",
	})

	// Start the notification service
	ns.OnNotificationEvent(func(changes database.RowStruct) {
		var oldValue Product
		var product Product

		ns.Scan(changes.OldValues, &oldValue)
		ns.Scan(changes.OldValues, &product)

		fmt.Println("OldValues:", oldValue)
		fmt.Println("NewValues:", product)
	})
}
