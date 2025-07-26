-- Очистка таблицы
TRUNCATE TABLE properties;

-- Загрузка данных из CSV файла
COPY properties (id, title, price, latitude, longitude, property_type, rooms, area)
FROM '/var/lib/postgresql/data/ads_202507261528.csv'
WITH (
    FORMAT csv,
    HEADER true,
    FORCE_NULL (rooms, area),
    FORCE_NOT_NULL (price, latitude, longitude)
)
WHERE geo_lat IS NOT NULL 
  AND geo_lng IS NOT NULL 
  AND price IS NOT NULL 
  AND price > 0;

-- Обновление данных для соответствия структуре таблицы
UPDATE properties 
SET 
    title = COALESCE(name, 'Объект недвижимости #' || id),
    property_type = CASE 
        WHEN realty_type_id = 1 THEN 'Квартира'
        WHEN realty_type_id = 2 THEN 'Дом'
        WHEN realty_type_id = 3 THEN 'Коммерческая'
        ELSE 'Другое'
    END,
    rooms = CASE 
        WHEN rooms_type_id IS NOT NULL THEN rooms_type_id
        ELSE NULL
    END,
    area = total_area
WHERE id IN (SELECT id FROM properties);

-- Создание индексов для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_properties_coordinates ON properties USING GIST (
    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
);

-- Статистика
SELECT COUNT(*) as total_properties FROM properties; 