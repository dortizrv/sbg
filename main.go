package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

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

type Inventory struct {
	InventoryID  int       `json:"inventory_id"`
	ProductID    int       `json:"product_id"`
	WhareHouseId int       `json:"warehouse_id"`
	QtyEntered   float64   `json:"qty_entered"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type JsonResult struct {
	Row struct {
		OldValues map[string]interface{} `json:"OldValues"`
		NewValues map[string]interface{} `json:"NewValues"`
	} `json:"row"`
}

func main() {
	var err error
	connString := "server=10.1.14.16;user id=sa;password=HT3dcwb730!$;database=ecommerce;schema=dbo;"
	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error opening the database:", err)
	}
	defer db.Close()

	fmt.Println("Runing service")

	if db.Ping() != nil {
		log.Fatal("Error ping to database server: ", err)
	}

	fmt.Println("Connection established successfully")

	ns := database.SqlNotificationService{}

	// Set database configuration
	ns.SetSetting(db, database.SettingNotification{
		Schema:    "dbo",
		TableName: "product",
		// Queue:       "ChangeInventoryQueue",
		// MessageType: "OnUpdateInventory",
		// Contract:    "InventoryProcessingContract",
		// ServiceName: "ChangeInventoryService",
		// EventName:   "ChangeInventory",
	})
	ns.Close()

	// Start the notification service
	ns.OnNotificationEvent(func(changes database.RowStruct) {
		var oldValue Product
		var product Product

		ns.Scan(changes.OldValues, &oldValue)
		ns.Scan(changes.NewValues, &product)

		fmt.Println("OldValues:", oldValue)
		fmt.Println("NewValues:", product)

		// if (oldValue.Price != product.Price) || (oldValue.Name != product.Name) {
		// 	fmt.Println("Price or Name changed")
		// }
	})
}
