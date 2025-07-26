package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
)

type Cluster struct {
	ClusterID  string  `json:"cluster_id"`
	CenterLat  float64 `json:"center_lat"`
	CenterLng  float64 `json:"center_lng"`
	PointCount int     `json:"point_count"`
	AvgPrice   float64 `json:"avg_price"`
	MinPrice   float64 `json:"min_price"`
	MaxPrice   float64 `json:"max_price"`
}

type Property struct {
	ID           int      `json:"id"`
	Title        string   `json:"title"`
	Price        float64  `json:"price"`
	Latitude     float64  `json:"latitude"`
	Longitude    float64  `json:"longitude"`
	PropertyType string   `json:"property_type"`
	Rooms        *int     `json:"rooms"`
	Area         *float64 `json:"area"`
}

var db *sql.DB

func main() {
	// Подключение к базе данных
	dbHost := getEnv("DB_HOST", "localhost")
	dbPort := getEnv("DB_PORT", "5432")
	dbName := getEnv("DB_NAME", "real_estate")
	dbUser := getEnv("DB_USER", "postgres")
	dbPassword := getEnv("DB_PASSWORD", "password")

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Ошибка подключения к базе данных:", err)
	}
	defer db.Close()

	// Проверка подключения
	err = db.Ping()
	if err != nil {
		log.Fatal("Ошибка проверки подключения к базе данных:", err)
	}

	log.Println("Подключение к базе данных успешно установлено")

	// Настройка Gin
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	// Статические файлы
	r.Static("/static", "./static")
	r.LoadHTMLGlob("templates/*")

	// Маршруты
	r.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.html", gin.H{
			"title": "Кластеризация недвижимости",
		})
	})

	// API для получения кластеров
	r.GET("/api/clusters", getClustersHandler)

	// API для получения отдельных объектов в кластере
	r.GET("/api/properties", getPropertiesHandler)

	log.Println("Сервер запущен на порту 8080")
	r.Run(":8080")
}

func getClustersHandler(c *gin.Context) {
	minLat, _ := strconv.ParseFloat(c.Query("min_lat"), 64)
	maxLat, _ := strconv.ParseFloat(c.Query("max_lat"), 64)
	minLng, _ := strconv.ParseFloat(c.Query("min_lng"), 64)
	maxLng, _ := strconv.ParseFloat(c.Query("max_lng"), 64)
	zoomLevel, _ := strconv.Atoi(c.Query("zoom"))

	if minLat == 0 || maxLat == 0 || minLng == 0 || maxLng == 0 || zoomLevel == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Неверные параметры"})
		return
	}

	clusters, err := getClusters(minLat, maxLat, minLng, maxLng, zoomLevel)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, clusters)
}

func getPropertiesHandler(c *gin.Context) {
	minLat, _ := strconv.ParseFloat(c.Query("min_lat"), 64)
	maxLat, _ := strconv.ParseFloat(c.Query("max_lat"), 64)
	minLng, _ := strconv.ParseFloat(c.Query("min_lng"), 64)
	maxLng, _ := strconv.ParseFloat(c.Query("max_lng"), 64)
	limit, _ := strconv.Atoi(c.Query("limit"))

	if limit == 0 {
		limit = 1000 // Ограничение по умолчанию
	}

	properties, err := getProperties(minLat, maxLat, minLng, maxLng, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, properties)
}

func getClusters(minLat, maxLat, minLng, maxLng float64, zoomLevel int) ([]Cluster, error) {
	fmt.Println(minLat, maxLat, minLng, maxLng, zoomLevel)
	query := `
WITH
params AS (
    SELECT 
        82.8::double precision AS min_lng,
        54.7::double precision AS min_lat,
        83.2::double precision AS max_lng,
        55.2::double precision AS max_lat,
        $5 AS zoom
),
constants AS (
    SELECT 
        20037508.34 * 2 AS world_size_merc,
        256 * (20037508.34 * 2 / (256 * (1 << zoom::integer))) AS tile_resolution,
        ST_Transform(
            ST_MakeEnvelope($1, $2, $3, $4, 4326),
            3857
        ) AS bbox_geom
    FROM params
),
raw_points AS (
    SELECT 
        p.id, 
        p.price,
        ST_Transform(ST_SetSRID(ST_MakePoint(p.longitude, p.latitude), 4326), 3857) AS geom
    FROM properties p, constants c
    WHERE ST_Transform(ST_SetSRID(ST_MakePoint(p.longitude, p.latitude), 4326), 3857) && c.bbox_geom
),
tile_grid AS (
    SELECT 
        ST_SnapToGrid(r.geom, c.tile_resolution, c.tile_resolution) AS grid_cell,
        r.price,
        r.geom
    FROM raw_points r, constants c
),
clusters AS (
    SELECT
        ST_AsText(grid_cell) AS cluster_id,
        COUNT(*) AS point_count,
        AVG(price) AS avg_price,
        MIN(price) AS min_price,
        MAX(price) AS max_price,
        ST_Centroid(ST_Collect(geom)) AS center_geom
    FROM tile_grid
    GROUP BY grid_cell
)
SELECT
    cluster_id,
    ST_Y(ST_Transform(center_geom, 4326)) AS center_lat,
    ST_X(ST_Transform(center_geom, 4326)) AS center_lng,
    point_count,
    avg_price,
    min_price,
    max_price
FROM clusters;
`

	zoomLevel = zoomLevel + 2
	rows, err := db.Query(query, minLng, minLat, maxLng, maxLat, zoomLevel)
	if err != nil {
		log.Println("Ошибка при выполнении запроса:", err)
		return nil, err
	}
	defer rows.Close()

	var clusters []Cluster
	for rows.Next() {
		var cluster Cluster
		err := rows.Scan(
			&cluster.ClusterID,
			&cluster.CenterLat,
			&cluster.CenterLng,
			&cluster.PointCount,
			&cluster.AvgPrice,
			&cluster.MinPrice,
			&cluster.MaxPrice,
		)
		if err != nil {
			return nil, err
		}
		clusters = append(clusters, cluster)
	}

	return clusters, nil
}

func getProperties(minLat, maxLat, minLng, maxLng float64, limit int) ([]Property, error) {
	query := `
		SELECT 
			id, title, price, latitude, longitude, property_type, rooms, area
		FROM properties 
		WHERE latitude BETWEEN $1 AND $2 
		  AND longitude BETWEEN $3 AND $4
		LIMIT $5
	`

	rows, err := db.Query(query, minLat, maxLat, minLng, maxLng, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var properties []Property
	for rows.Next() {
		var prop Property
		err := rows.Scan(
			&prop.ID,
			&prop.Title,
			&prop.Price,
			&prop.Latitude,
			&prop.Longitude,
			&prop.PropertyType,
			&prop.Rooms,
			&prop.Area,
		)
		if err != nil {
			return nil, err
		}
		properties = append(properties, prop)
	}

	return properties, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
