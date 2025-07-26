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

CREATE OR REPLACE FUNCTION generate_weighted_random_properties(count INTEGER) RETURNS VOID AS $$
DECLARE
    i INTEGER;
    lat DOUBLE PRECISION;
    lng DOUBLE PRECISION;
    prop_type VARCHAR(50);
    room_count INTEGER;
    price_val DOUBLE PRECISION;
    area_val DOUBLE PRECISION;

    -- Координаты районов
    areas JSON := '[
        {"name": "Центральный",     "lat": 55.03, "lng": 82.92, "weight": 5},
        {"name": "Октябрьский",     "lat": 54.99, "lng": 82.95, "weight": 4},
        {"name": "Плющихинский",    "lat": 54.99, "lng": 82.85, "weight": 2},
        {"name": "Родники",         "lat": 55.10, "lng": 83.05, "weight": 2},
        {"name": "Советский",       "lat": 54.85, "lng": 83.10, "weight": 1}
    ]';
    total_weight INTEGER := 0;
    chosen_area JSON;
    rand FLOAT;
    sum FLOAT := 0;
BEGIN
    -- Считаем сумму всех весов
    FOR i IN 0 .. json_array_length(areas) - 1 LOOP
        total_weight := total_weight + (areas -> i ->> 'weight')::INTEGER;
    END LOOP;

    FOR i IN 1..count LOOP
        -- Выбираем район на основе веса
        rand := random() * total_weight;
        sum := 0;

        FOR j IN 0 .. json_array_length(areas) - 1 LOOP
            sum := sum + (areas -> j ->> 'weight')::FLOAT;
            IF rand <= sum THEN
                chosen_area := areas -> j;
                EXIT;
            END IF;
        END LOOP;

        -- Координаты с разбросом ±0.01
        lat := (chosen_area ->> 'lat')::FLOAT + ((random() - 0.5) * 0.02);
        lng := (chosen_area ->> 'lng')::FLOAT + ((random() - 0.5) * 0.02);

        -- Тип недвижимости
        prop_type := CASE (random() * 3)::INTEGER
            WHEN 0 THEN 'Квартира'
            WHEN 1 THEN 'Дом'
            WHEN 2 THEN 'Коммерческая'
            ELSE 'Квартира'
        END;

        -- Кол-во комнат
        IF prop_type IN ('Квартира', 'Дом') THEN
            room_count := 1 + (random() * 5)::INTEGER;
        ELSE
            room_count := NULL;
        END IF;

        price_val := 1000000 + (random() * 20000000);
        area_val := 20 + (random() * 200);

        INSERT INTO properties (title, price, latitude, longitude, property_type, rooms, area)
        VALUES (
            prop_type || ' в ' || (chosen_area ->> 'name') || ' #' || i,
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


-- Вызов:
SELECT generate_weighted_random_properties(100000);
