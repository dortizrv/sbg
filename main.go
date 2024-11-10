package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"

	xj "github.com/basgys/goxml2json"
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
	ns.OnNotificationEvent(func(changes string) {
		xml := strings.NewReader(changes)
		json, err := xj.Convert(xml)
		if err != nil {
			log.Fatal(err)
		}

		var result JsonResult
		var product Product
		jsonToStruct(json.String(), &result)
		scanRow(result.Row.NewValues, &product)

		// result := jsonToMapRecursive(json.String())["row"]
		fmt.Println("CD", product)
	})

	// for {
	// 	receiveMessage()
	// 	time.Sleep(5 * time.Second) // Wait before the next query
	// }
}

// func receiveMessage() {
// 	var messageBody string
// 	query := "WAITFOR (RECEIVE TOP(1) CONVERT(XML, message_body) AS message_body FROM [ChangeNotificationQueue]);"
// 	fmt.Println("Waiting for changes...")
// 	err := db.QueryRow(query).Scan(&messageBody)
// 	if err != nil && err != sql.ErrNoRows {
// 		log.Println("Error receiving the message:", err)
// 		return
// 	}
// 	if messageBody != "" {
// 		fmt.Println("Message received:", messageBody)
// 	}
// }

func scanRow(r interface{}, v interface{}) {
	t := reflect.ValueOf(r)
	rV := reflect.TypeOf(v)

	keys := t.MapKeys()
	validFieldCount := 0
	for _, k := range keys {
		// result := t.MapIndex(k)

		if rV.Kind() == reflect.Ptr {
			for ind := 0; ind < rV.Elem().NumField(); ind++ {
				// Asigna el valor de r a v
				field := rV.Elem().Field(ind)

				tag := field.Tag.Get("json")

				if tag == k.String() {
					validFieldCount++
					fmt.Println("sumando andamos")
					// field.Set(t.MapIndex(k).Interface())
					rValue := reflect.ValueOf(v)
					fields := rValue.Elem().FieldByName(field.Name)
					fmt.Println("field:", fields.Type(), field.Type)

					if fields.Type() == field.Type {
						fmt.Println("es igual")
						newValue := reflect.ValueOf(t.MapIndex(k).Interface())
						newType := reflect.TypeOf(t.MapIndex(k).Interface())

						fmt.Println(newValue, newType)
						if fields.Type() != newType {
							fmt.Println("no es igual", newType)
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

func jsonToStruct(jsonStr string, t interface{}) {
	// var result t.Type
	json.Unmarshal([]byte(jsonStr), &t)
}

// func jsonToMapRecursive(jsonStr string) map[string]interface{} {
// 	var result map[string]interface{}
// 	json.Unmarshal([]byte(jsonStr), &result)
// 	recursiveTransform(result)
// 	return result
// }

// func recursiveTransform(data interface{}) interface{} {
// 	switch v := data.(type) {
// 	case map[string]interface{}:
// 		for key, value := range v {
// 			v[key] = recursiveTransform(value)
// 		}
// 	case []interface{}:
// 		for i, value := range v {
// 			v[i] = recursiveTransform(value)
// 		}
// 	}
// 	return data
// }
