package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/nats-io/stan.go"
)

func main() {
	sc, err := stan.Connect("test-cluster", "publisher", stan.NatsURL(stan.DefaultNatsURL))
	if err != nil {
		log.Fatal(err)
	}
	defer sc.Close()

	// Базовый заказ
	baseOrder := map[string]interface{}{
		"order_uid":    "b563feb7b2b84b6test",
		"track_number": "WBILMTESTTRACK",
		"entry":        "WBIL",
		"delivery": map[string]interface{}{
			"name":    "Test Testov",
			"phone":   "+9720000000",
			"zip":     "2639809",
			"city":    "Kiryat Mozkin",
			"address": "Ploshad Mira 15",
			"region":  "Kraiot",
			"email":   "test@gmail.com",
		},
		"payment": map[string]interface{}{
			"transaction":  "b563feb7b2b84b6test",
			"request_id":   "",
			"currency":     "USD",
			"provider":     "wbpay",
			"amount":       1817,
			"payment_dt":   1637907727,
			"bank":         "alpha",
			"delivery_cost": 1500,
			"goods_total":  317,
			"custom_fee":   0,
		},
		"items": []map[string]interface{}{
			{
				"chrt_id":      9934930,
				"track_number": "WBILMTESTTRACK",
				"price":        453,
				"rid":          "ab4219087a764ae0btest",
				"name":         "Mascaras",
				"sale":         30,
				"size":         "0",
				"total_price":  317,
				"nm_id":        2389212,
				"brand":        "Vivienne Sabo",
				"status":       202,
			},
		},
		"locale":             "en",
		"internal_signature": "",
		"customer_id":        "test",
		"delivery_service":   "meest",
		"shardkey":           "9",
		"sm_id":              99,
		"date_created":       "2021-11-26T06:22:19Z",
		"oof_shard":          "1",
	}

	// Отправка тестового заказа
	data, err := json.Marshal(baseOrder)
	if err != nil {
		log.Fatal("Ошибка маршалинга:", err)
	}

	err = sc.Publish("orders", data)
	if err != nil {
		log.Fatal("Ошибка отправки:", err)
	}
	log.Println("✅ Тестовый заказ отправлен в NATS!")

	// Опционально: отправка нескольких заказов
	fmt.Print("Отправить несколько тестовых заказов? (y/n): ")
	var answer string
	fmt.Scanln(&answer)
	
	if answer == "y" || answer == "Y" {
		sendMultipleOrders(sc)
	}
}

func sendMultipleOrders(sc stan.Conn) {
	rand.Seed(time.Now().UnixNano())
	
	for i := 1; i <= 5; i++ {
		order := map[string]interface{}{
			"order_uid":    fmt.Sprintf("test_order_%d_%d", time.Now().Unix(), i),
			"track_number": fmt.Sprintf("TRACK%d", 1000+i),
			"entry":        "WBIL",
			"delivery": map[string]interface{}{
				"name":    fmt.Sprintf("User %d", i),
				"phone":   "+7900123456" + fmt.Sprintf("%d", i),
				"zip":     "12345" + fmt.Sprintf("%d", i),
				"city":    "Moscow",
				"address": fmt.Sprintf("Street %d", i),
				"region":  "Central",
				"email":   fmt.Sprintf("user%d@test.com", i),
			},
			"payment": map[string]interface{}{
				"transaction":   fmt.Sprintf("trans_%d", i),
				"currency":      "USD",
				"provider":      "test_provider",
				"amount":        1000 + i*100,
				"payment_dt":    time.Now().Unix(),
				"delivery_cost": 500,
				"goods_total":   500 + i*100,
			},
			"items": []map[string]interface{}{
				{
					"chrt_id":     9000 + i,
					"track_number": fmt.Sprintf("ITEMTRACK%d", i),
					"price":       100 + i*10,
					"name":        fmt.Sprintf("Product %d", i),
					"brand":       "Test Brand",
					"status":      202,
				},
			},
			"customer_id": fmt.Sprintf("customer_%d", i),
			"date_created": time.Now().Format(time.RFC3339),
		}

		data, err := json.Marshal(order)
		if err != nil {
			log.Printf("Ошибка маршалинга заказа %d: %v", i, err)
			continue
		}

		err = sc.Publish("orders", data)
		if err != nil {
			log.Printf("Ошибка отправки заказа %d: %v", i, err)
			continue
		}

		log.Printf("✅ Дополнительный заказ %d отправлен", i)
		time.Sleep(500 * time.Millisecond) // Небольшая задержка
	}
}