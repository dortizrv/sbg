// sbg_test.go - Pruebas unitarias para la notificacion de cambios en una tabla de SQL Server

package main

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/denisenkom/go-mssqldb"
)

var db *sql.DB

// Estructura para la tabla product
type Product struct {
	ProductID int     `json:"product_id"`
	Sku       string  `json:"sku"`
	Name      string  `json:"name"`
	Price     float64 `json:"price"`
}

// TestOnDetectChange - Verifica que el servicio detecte cambios en la tabla product
func TestOnDetectChange(t *testing.T) {
	var err error
	connString := "server=10.1.14.16;user id=sa;password=HT3dcwb730!$;database=ecommerce;schema=dbo;"
	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		t.Error("Error opening the database:", err)
	}
	defer db.Close()

	if db.Ping() != nil {
		t.Error("Error ping to database server: ", err)
	}

	fmt.Println("Connection established successfully")
	fmt.Println("Waiting for changes...")

	ns := SqlNotificationService{}

	// Configura el servicio de notificaciones
	ns.SetSetting(db, SettingNotification{
		Schema:    "dbo",
		TableName: "product",
	})

	// Start the notification service
	ns.OnNotificationEvent(func(changes RowStruct) {
		var oldValue Product
		var product Product

		ns.Scan(changes.OldValues, &oldValue)
		ns.Scan(changes.NewValues, &product)

		fmt.Println("OldValues:", oldValue)
		fmt.Println("NewValues:", product)
	})
}
