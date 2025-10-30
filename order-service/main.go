package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/stan.go"
)

type OrderCache struct {
	mu     sync.RWMutex
	orders map[string]json.RawMessage
}

func NewOrderCache() *OrderCache {
	return &OrderCache{
		orders: make(map[string]json.RawMessage),
	}
}

func (c *OrderCache) Set(uid string, data json.RawMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.orders[uid] = data
}

func (c *OrderCache) Get(uid string) (json.RawMessage, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	data, ok := c.orders[uid]
	return data, ok
}

func (c *OrderCache) GetAll() map[string]json.RawMessage {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	// Создаем копию для безопасного доступа
	result := make(map[string]json.RawMessage, len(c.orders))
	for k, v := range c.orders {
		result[k] = v
	}
	return result
}

func (c *OrderCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.orders)
}

// validateOrder проверяет минимальную валидность JSON заказа
func validateOrder(data []byte) (string, error) {
	var order struct {
		OrderUID string `json:"order_uid"`
	}
	
	if err := json.Unmarshal(data, &order); err != nil {
		return "", fmt.Errorf("invalid JSON: %v", err)
	}
	
	if order.OrderUID == "" {
		return "", fmt.Errorf("order_uid is required")
	}
	
	return order.OrderUID, nil
}

func main() {
	// Конфигурация (можно вынести в отдельный файл)
	const (
		dbURL          = "postgres://orderuser:orderpass@localhost:5432/orderdb?sslmode=disable"
		natsClusterID  = "test-cluster"
		natsClientID   = "order-service"
		natsSubject    = "orders"
		httpAddr       = ":8080"
	)

	// Подключение к БД
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		log.Fatal("Ошибка подключения к БД:", err)
	}
	defer db.Close()

	// Проверка подключения к БД
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	if err := db.PingContext(ctx); err != nil {
		log.Fatal("Не удалось подключиться к БД:", err)
	}

	// Инициализация кэша
	cache := NewOrderCache()

	// Восстановление кэша из БД
	log.Println("Восстановление кэша из БД...")
	rows, err := db.Query("SELECT order_uid, raw_data FROM orders")
	if err != nil {
		log.Printf("Предупреждение: не удалось загрузить заказы из БД: %v", err)
	} else {
		defer rows.Close()
		
		count := 0
		for rows.Next() {
			var uid string
			var data json.RawMessage
			if err := rows.Scan(&uid, &data); err != nil {
				log.Printf("Ошибка при чтении строки: %v", err)
				continue
			}
			cache.Set(uid, data)
			count++
		}
		
		if err := rows.Err(); err != nil {
			log.Printf("Ошибка при итерации по строкам: %v", err)
		}
		
		log.Printf("Кэш восстановлен: %d заказов", count)
	}

	// Подключение к NATS Streaming
	sc, err := stan.Connect(natsClusterID, natsClientID, stan.NatsURL(stan.DefaultNatsURL))
	if err != nil {
		log.Fatal("Ошибка подключения к NATS:", err)
	}
	defer sc.Close()

	// Подписка на канал
	sub, err := sc.Subscribe(natsSubject, func(msg *stan.Msg) {
		log.Printf("Получено сообщение: %d байт", len(msg.Data))
		
		// Валидация заказа
		uid, err := validateOrder(msg.Data)
		if err != nil {
			log.Printf("❌ Невалидный заказ: %v", err)
			return
		}

		// Сохранение в БД
		_, err = db.Exec(
			"INSERT INTO orders (order_uid, raw_data) VALUES ($1, $2) ON CONFLICT (order_uid) DO UPDATE SET raw_data = $2",
			uid, msg.Data,
		)
		if err != nil {
			log.Printf("❌ Ошибка записи заказа %s: %v", uid, err)
			return
		}

		// Сохранение в кэш
		cache.Set(uid, json.RawMessage(msg.Data))
		log.Printf("✅ Заказ %s обработан", uid)
	}, 
	stan.DurableName("order-durable"),
	stan.SetManualAckMode(),
	stan.AckWait(30*time.Second),
	)
	
	if err != nil {
		log.Fatal("Ошибка подписки:", err)
	}
	defer sub.Unsubscribe()

	// HTTP сервер
	router := mux.NewRouter()

	// Главная страница
	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		
		orders := cache.GetAll()
		html := `<!DOCTYPE html>
		<html>
		<head>
			<title>Демонстрационный сервис заказов</title>
			<meta charset="utf-8">
			<style>
				body { font-family: Arial, sans-serif; margin: 40px; }
				.order-list { max-height: 400px; overflow-y: auto; border: 1px solid #ddd; padding: 10px; }
				.order-item { margin: 5px 0; }
				.stats { background: #f5f5f5; padding: 10px; border-radius: 5px; }
				pre { background: #f8f8f8; padding: 10px; border-radius: 5px; overflow-x: auto; }
			</style>
		</head>
		<body>
			<h2>📦 Демонстрационный сервис заказов</h2>
			
			<div class="stats">
				<strong>Статистика:</strong>
				<p>Всего заказов в кэше: ` + fmt.Sprintf("%d", len(orders)) + `</p>
				<p>Время: ` + time.Now().Format("15:04:05") + `</p>
			</div>

			<h3>Список заказов:</h3>
			<div class="order-list">
				<ul>`
		
		for uid := range orders {
			html += `<li class="order-item">
				<a href="/order/` + uid + `" target="_blank">` + uid + `</a>
				| <a href="/order/` + uid + `?format=pretty" target="_blank">(красиво)</a>
			</li>`
		}
		
		html += `</ul>
			</div>
			
			<hr>
			<h3>API Endpoints:</h3>
			<ul>
				<li><a href="/health">Health Check</a></li>
				<li><a href="/orders">Все заказы (JSON)</a></li>
			</ul>
			
			<h3>Как добавить заказ?</h3>
			<pre>go run publish.go</pre>
			
			<h3>Пример curl запроса:</h3>
			<pre>curl http://localhost:8080/order/b563feb7b2b84b6test</pre>
		</body>
		</html>`
		
		w.Write([]byte(html))
	})

	// Получение заказа по ID
	router.HandleFunc("/order/{uid}", func(w http.ResponseWriter, r *http.Request) {
		uid := mux.Vars(r)["uid"]
		
		data, ok := cache.Get(uid)
		if !ok {
			http.Error(w, `{"error": "Заказ не найден"}`, http.StatusNotFound)
			return
		}

		// Красивый вывод если запрошен
		if r.URL.Query().Get("format") == "pretty" {
			var prettyJSON bytes.Buffer
			if err := json.Indent(&prettyJSON, data, "", "  "); err != nil {
				http.Error(w, `{"error": "Ошибка форматирования JSON"}`, http.StatusInternalServerError)
				return
			}
			
			w.Header().Set("Content-Type", "application/json")
			w.Write(prettyJSON.Bytes())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	})

	// Получение всех заказов
	router.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		orders := cache.GetAll()
		
		response := map[string]interface{}{
			"count":  len(orders),
			"orders": orders,
		}
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})

	// Health check
	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		status := map[string]interface{}{
			"status":    "healthy",
			"timestamp": time.Now().UTC(),
			"cache_size": cache.Size(),
		}
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	})

	// Запуск HTTP сервера с graceful shutdown
	server := &http.Server{
		Addr:    httpAddr,
		Handler: router,
	}

	// Канал для graceful shutdown
	done := make(chan bool, 1)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("Получен сигнал завершения...")
		
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("Ошибка при завершении сервера: %v", err)
		}
		
		close(done)
	}()

	log.Println("🚀 Сервис запущен на http://localhost:8080")
	log.Println("Нажмите Ctrl+C для остановки")

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Ошибка HTTP сервера: %v", err)
	}

	<-done
	log.Println("Сервис остановлен")
}