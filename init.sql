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