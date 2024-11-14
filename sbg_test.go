// sbg_test.go - Pruebas unitarias para la notificacion de cambios en una tabla de SQL Server

package sbg_test

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/dortizrv/sbg"
)

var db *sql.DB

// Estructura para la tabla product
type Product struct {
	ProductID int     `json:"ID"`
	Sku       string  `json:"C_Codigo"`
	Name      string  `json:"C_Descri"`
	Price     float64 `json:"n_Precio1"`
}

// TestOnDetectChange - Verifica que el servicio detecte cambios en la tabla product
func TestOnDetectChange(t *testing.T) {
	var err error
	connString := "server=10.1.14.6\\testing;user id=dortiz;password=asdf.123;database=VAD10;schema=dbo;"
	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		t.Error("Error opening the database:", err)
	}
	defer db.Close()

	if db.Ping() != nil {
		t.Error("Error ping to database server: ", err)
	} else {
		fmt.Println("Connection established successfully")
		fmt.Println("Waiting for changes...")
	}

	// ns := sbg.SqlNotificationService{}

	ns := sbg.Sbg()

	// Configura el servicio de notificaciones
	ns.SetSetting(db, sbg.SettingNotification{
		Schema:    "dbo",
		TableName: "MA_PRODUCTOS",
	})

	// Start the notification service
	ns.OnNotificationEvent(func(changes sbg.RowStruct) {
		var oldValue Product
		var product Product

		ns.Scan(changes.OldValues, &oldValue)
		ns.Scan(changes.NewValues, &product)

		fmt.Println("OldValues:", oldValue)
		fmt.Println("NewValues:", product)
	})
}
