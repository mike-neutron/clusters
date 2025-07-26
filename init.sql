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

-- Индекс для быстрого поиска по координатам в Web Mercator
CREATE INDEX IF NOT EXISTS idx_properties_mercator ON properties 
USING GIST (ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326), 3857));