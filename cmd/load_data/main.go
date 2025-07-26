package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

type CSVProperty struct {
	ID           int
	GeoLat       float64
	GeoLng       float64
	Price        float64
	Name         string
	RoomsTypeID  *int
	TotalArea    *float64
	RealtyTypeID int
}

func main() {
	// Подключение к базе данных
	db, err := sql.Open("postgres", "host=localhost port=5432 user=postgres password=password dbname=real_estate sslmode=disable")
	if err != nil {
		log.Fatal("Ошибка подключения к базе данных:", err)
	}
	defer db.Close()

	// Проверка подключения
	err = db.Ping()
	if err != nil {
		log.Fatal("Ошибка проверки подключения:", err)
	}
	fmt.Println("Подключение к базе данных установлено")

	// Очистка таблицы
	_, err = db.Exec("TRUNCATE TABLE properties")
	if err != nil {
		log.Fatal("Ошибка очистки таблицы:", err)
	}
	fmt.Println("Таблица очищена")

	// Открытие CSV файла
	file, err := os.Open("ads_202507261528.csv")
	if err != nil {
		log.Fatal("Ошибка открытия файла:", err)
	}
	defer file.Close()

	// Создание CSV reader
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 // Разрешаем разное количество полей

	// Чтение заголовка
	header, err := reader.Read()
	if err != nil {
		log.Fatal("Ошибка чтения заголовка:", err)
	}

	// Поиск индексов нужных колонок
	indices := make(map[string]int)
	for i, col := range header {
		indices[strings.Trim(col, `"`)] = i
	}

	// Проверка наличия нужных колонок
	required := []string{"id", "geo_lat", "geo_lng", "price", "name", "rooms_type_id", "total_area", "realty_type_id"}
	for _, col := range required {
		if _, exists := indices[col]; !exists {
			log.Fatalf("Колонка %s не найдена в CSV файле", col)
		}
	}

	fmt.Printf("Найдены колонки: %v\n", indices)

	// Параллельная обработка
	numWorkers := 8 // Количество горутин
	batchSize := 1000
	batchChan := make(chan []CSVProperty, numWorkers*2)
	var wg sync.WaitGroup
	var totalInserted int64
	var mu sync.Mutex

	startTime := time.Now()

	// Запуск горутин для вставки
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Создаем отдельное подключение для каждой горутины
			workerDB, err := sql.Open("postgres", "host=localhost port=5432 user=postgres password=password dbname=real_estate sslmode=disable")
			if err != nil {
				log.Printf("Ошибка подключения горутины %d: %v", workerID, err)
				return
			}
			defer workerDB.Close()

			for batch := range batchChan {
				inserted, err := insertBatch(workerDB, batch)
				if err != nil {
					log.Printf("Ошибка вставки пачки в горутине %d: %v", workerID, err)
				} else {
					mu.Lock()
					totalInserted += int64(inserted)
					currentTotal := totalInserted
					mu.Unlock()

					elapsed := time.Since(startTime)
					rate := float64(currentTotal) / elapsed.Seconds()
					fmt.Printf("Горутина %d: вставлено %d записей, всего: %d (%.0f записей/сек)\n",
						workerID, inserted, currentTotal, rate)
				}
			}
		}(i)
	}

	// Чтение и отправка данных в канал
	batch := make([]CSVProperty, 0, batchSize)
	recordsProcessed := 0

	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			log.Printf("Ошибка чтения строки: %v", err)
			continue
		}

		recordsProcessed++
		if recordsProcessed%10000 == 0 {
			fmt.Printf("Обработано записей: %d\n", recordsProcessed)
		}

		// Парсинг данных
		prop, err := parseProperty(record, indices)
		if err != nil {
			log.Printf("Ошибка парсинга строки: %v", err)
			continue
		}

		// Фильтрация данных
		if prop.GeoLat == 0 || prop.GeoLng == 0 || prop.Price <= 0 {
			continue
		}

		batch = append(batch, prop)

		// Отправка пачки в канал
		if len(batch) >= batchSize {
			batchCopy := make([]CSVProperty, len(batch))
			copy(batchCopy, batch)
			batchChan <- batchCopy
			batch = batch[:0] // Очистка слайса
		}
	}

	// Отправка оставшихся записей
	if len(batch) > 0 {
		batchChan <- batch
	}

	// Закрытие канала и ожидание завершения горутин
	close(batchChan)
	wg.Wait()

	elapsed := time.Since(startTime)
	fmt.Printf("Загрузка завершена за %v. Всего вставлено: %d записей (%.0f записей/сек)\n",
		elapsed, totalInserted, float64(totalInserted)/elapsed.Seconds())
}

func parseProperty(record []string, indices map[string]int) (CSVProperty, error) {
	var prop CSVProperty
	var err error

	// ID
	prop.ID, err = strconv.Atoi(strings.Trim(record[indices["id"]], `"`))
	if err != nil {
		return prop, fmt.Errorf("ошибка парсинга ID: %v", err)
	}

	// Координаты
	latStr := strings.Trim(record[indices["geo_lat"]], `"`)
	if latStr != "" {
		prop.GeoLat, err = strconv.ParseFloat(latStr, 64)
		if err != nil {
			return prop, fmt.Errorf("ошибка парсинга latitude: %v", err)
		}
	}

	lngStr := strings.Trim(record[indices["geo_lng"]], `"`)
	if lngStr != "" {
		prop.GeoLng, err = strconv.ParseFloat(lngStr, 64)
		if err != nil {
			return prop, fmt.Errorf("ошибка парсинга longitude: %v", err)
		}
	}

	// Цена
	priceStr := strings.Trim(record[indices["price"]], `"`)
	if priceStr != "" {
		prop.Price, err = strconv.ParseFloat(priceStr, 64)
		if err != nil {
			return prop, fmt.Errorf("ошибка парсинга price: %v", err)
		}
	}

	// Название
	prop.Name = strings.Trim(record[indices["name"]], `"`)

	// Количество комнат
	roomsStr := strings.Trim(record[indices["rooms_type_id"]], `"`)
	if roomsStr != "" {
		rooms, err := strconv.Atoi(roomsStr)
		if err == nil {
			prop.RoomsTypeID = &rooms
		}
	}

	// Площадь
	areaStr := strings.Trim(record[indices["total_area"]], `"`)
	if areaStr != "" {
		area, err := strconv.ParseFloat(areaStr, 64)
		if err == nil {
			prop.TotalArea = &area
		}
	}

	// Тип недвижимости
	realtyStr := strings.Trim(record[indices["realty_type_id"]], `"`)
	if realtyStr != "" {
		prop.RealtyTypeID, err = strconv.Atoi(realtyStr)
		if err != nil {
			prop.RealtyTypeID = 1 // По умолчанию квартира
		}
	}

	return prop, nil
}

func insertBatch(db *sql.DB, batch []CSVProperty) (int, error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	inserted := 0
	for _, prop := range batch {
		// Определение типа недвижимости
		propertyType := "Другое"
		switch prop.RealtyTypeID {
		case 1:
			propertyType = "Квартира"
		case 2:
			propertyType = "Дом"
		case 3:
			propertyType = "Коммерческая"
		}

		// Название
		title := prop.Name
		if title == "" {
			title = fmt.Sprintf("Объект недвижимости #%d", prop.ID)
		}

		_, err := tx.Exec(`
			INSERT INTO properties (id, title, price, latitude, longitude, property_type, rooms, area)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`, prop.ID, title, prop.Price, prop.GeoLat, prop.GeoLng, propertyType, prop.RoomsTypeID, prop.TotalArea)

		if err != nil {
			log.Printf("Ошибка вставки записи %d: %v", prop.ID, err)
			continue
		}
		inserted++
	}

	err = tx.Commit()
	if err != nil {
		return 0, err
	}

	return inserted, nil
}
