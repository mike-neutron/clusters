-- Создание таблицы недвижимости
CREATE TABLE IF NOT EXISTS properties (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    property_type VARCHAR(50) NOT NULL,
    rooms INTEGER,
    area DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Включение расширения PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

-- Создание индексов для быстрого поиска по координатам
CREATE INDEX IF NOT EXISTS idx_properties_coordinates ON properties USING GIST (
    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
);

-- Функция для генерации случайных точек в пределах Новосибирска
CREATE OR REPLACE FUNCTION generate_random_properties(count INTEGER) RETURNS VOID AS $$
DECLARE
    i INTEGER;
    lat DOUBLE PRECISION;
    lng DOUBLE PRECISION;
    prop_type VARCHAR(50);
    room_count INTEGER;
    price_val DOUBLE PRECISION;
    area_val DOUBLE PRECISION;
BEGIN
    -- Новосибирск: примерно от 54.7 до 55.2 по широте и от 82.8 до 83.2 по долготе
    FOR i IN 1..count LOOP
        -- Генерация случайных координат в пределах Новосибирска
        lat := 54.7 + (random() * 0.5);
        lng := 82.8 + (random() * 0.4);
        
        -- Случайный тип недвижимости
        prop_type := CASE (random() * 3)::INTEGER
            WHEN 0 THEN 'Квартира'
            WHEN 1 THEN 'Дом'
            WHEN 2 THEN 'Коммерческая'
            ELSE 'Квартира'
        END;
        
        -- Случайное количество комнат (для квартир и домов)
        IF prop_type IN ('Квартира', 'Дом') THEN
            room_count := 1 + (random() * 5)::INTEGER;
        ELSE
            room_count := NULL;
        END IF;
        
        -- Случайная цена
        price_val := 1000000 + (random() * 20000000);
        
        -- Случайная площадь
        area_val := 20 + (random() * 200);
        
        INSERT INTO properties (title, price, latitude, longitude, property_type, rooms, area)
        VALUES (
            prop_type || ' в Новосибирске #' || i,
            price_val,
            lat,
            lng,
            prop_type,
            room_count,
            area_val
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Генерация 100000 случайных объектов недвижимости
SELECT generate_random_properties(100000);

-- Создание функции для кластеризации по тайлам
CREATE OR REPLACE FUNCTION get_clusters(
    p_min_lat DOUBLE PRECISION,
    p_max_lat DOUBLE PRECISION,
    p_min_lng DOUBLE PRECISION,
    p_max_lng DOUBLE PRECISION,
    p_zoom_level INTEGER
) RETURNS TABLE (
    cluster_id INTEGER,
    center_lat DOUBLE PRECISION,
    center_lng DOUBLE PRECISION,
    point_count INTEGER,
    avg_price DOUBLE PRECISION,
    min_price DOUBLE PRECISION,
    max_price DOUBLE PRECISION
) AS $$
DECLARE
    v_tile_size DOUBLE PRECISION;
BEGIN
    -- Размер тайла зависит от уровня зума
    -- При большем зуме тайлы становятся меньше
    v_tile_size := CASE 
        WHEN p_zoom_level <= 10 THEN 0.1
        WHEN p_zoom_level <= 12 THEN 0.05
        WHEN p_zoom_level <= 14 THEN 0.02
        WHEN p_zoom_level <= 16 THEN 0.01
        WHEN p_zoom_level <= 18 THEN 0.005
        ELSE 0.002
    END;
    
    RETURN QUERY
    WITH tiles AS (
        SELECT 
            FLOOR((latitude - p_min_lat) / v_tile_size)::INTEGER as lat_tile,
            FLOOR((longitude - p_min_lng) / v_tile_size)::INTEGER as lng_tile,
            latitude,
            longitude,
            price
        FROM properties 
        WHERE latitude BETWEEN p_min_lat AND p_max_lat 
          AND longitude BETWEEN p_min_lng AND p_max_lng
    ),
    clusters AS (
        SELECT 
            lat_tile * 10000 + lng_tile as cid,
            AVG(latitude) as clat,
            AVG(longitude) as clng,
            COUNT(*)::INTEGER as pcount,
            AVG(price) as aprice,
            MIN(price) as minprice,
            MAX(price) as maxprice
        FROM tiles
        GROUP BY lat_tile, lng_tile
    )
    SELECT 
        cid as cluster_id,
        clat as center_lat,
        clng as center_lng,
        pcount as point_count,
        aprice as avg_price,
        minprice as min_price,
        maxprice as max_price
    FROM clusters
    ORDER BY cid;
END;
$$ LANGUAGE plpgsql; 