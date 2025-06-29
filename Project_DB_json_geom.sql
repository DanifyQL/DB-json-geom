-- Этап 1. Создание дополнительных таблиц

-- в дампе содержится информация только по 4-м типам заведений. Создадим для них отдельный тип 
CREATE TYPE cafe.restaurant_type AS ENUM ('coffee_shop', 'restaurant', 'bar', 'pizzeria');

-- создаем таблицу, которая будет хранить данные о ресторанах
CREATE TABLE IF NOT EXISTS cafe.restaurants
(
	-- генерация uuid
	restaurant_uuid uuid DEFAULT gen_random_uuid(),
	-- название заведения
	restaurant_name varchar(50), 
	-- координаты заведения
	restaurant_location geometry(POINT, 4326),
	-- тип заведения из ранее созданного типа
	cafe_type cafe.restaurant_type,
	-- автоматическое преобразование данных дампа из jsonb ->> json
	menu json, 
	PRIMARY KEY (restaurant_uuid)
);

-- создаем справочник с менеджерами
CREATE TABLE IF NOT EXISTS cafe.managers (
	manager_uuid uuid DEFAULT gen_random_uuid(),
	manager_name varchar(50) NOT NULL,
	phone text UNIQUE,
	PRIMARY KEY (manager_uuid)
);

-- создаем вспомогательную таблицу с датами работы менеджеров в ресторанах
CREATE TABLE cafe.restaurant_manager_work_dates(
	restaurant_uuid uuid,
	manager_uuid uuid,
	date_begin date DEFAULT CURRENT_DATE,
	date_end date,
	PRIMARY KEY (restaurant_uuid, manager_uuid),
	FOREIGN KEY (restaurant_uuid) REFERENCES cafe.restaurants(restaurant_uuid) ON DELETE CASCADE,
	FOREIGN KEY (manager_uuid) REFERENCES cafe.managers(manager_uuid) ON DELETE CASCADE
);

-- создаем таблицу с продажами по ресторанам
CREATE TABLE IF NOT EXISTS cafe.sales(
	date date,
	restaurant_uuid uuid,
	avg_check decimal(10, 2),
	PRIMARY KEY (date, restaurant_uuid),
	FOREIGN KEY (restaurant_uuid) REFERENCES cafe.restaurants(restaurant_uuid)
);

-- Запросы для заполнения таблиц
INSERT INTO cafe.restaurants(restaurant_name, restaurant_location, cafe_type, menu)
SELECT DISTINCT
	rds.cafe_name restaurant_name,
	ST_Point(rds.longitude, rds.latitude) restaurant_location,
	rds.type::cafe.restaurant_type,
	rdm.menu
FROM
	raw_data.sales rds
JOIN
	raw_data.menu rdm ON rdm.cafe_name = rds.cafe_name;


INSERT INTO cafe.managers(manager_name, phone)
SELECT DISTINCT
	rds.manager manager_name,
	rds.manager_phone phone
FROM
	raw_data.sales rds;


INSERT INTO cafe.restaurant_manager_work_dates(restaurant_uuid, manager_uuid, date_begin, date_end)
SELECT
	cr.restaurant_uuid restaurant_uuid,
	cm.manager_uuid manager_uuid,
	MIN(rds.report_date) date_begin,
	MAX(rds.report_date) date_end
FROM
	raw_data.sales rds
JOIN
	cafe.restaurants cr ON rds.cafe_name = cr.restaurant_name
JOIN
	cafe.managers cm ON rds.manager = cm.manager_name
GROUP BY
	restaurant_uuid,
	manager_uuid;


INSERT INTO cafe.sales(date, restaurant_uuid, avg_check)
SELECT
	rds.report_date date,
	cr.restaurant_uuid restaurant_uuid,
	rds.avg_check avg_check
FROM
	raw_data.sales rds
JOIN
	cafe.restaurants cr ON rds.cafe_name = cr.restaurant_name
GROUP BY
	date,
	restaurant_uuid,
	avg_check
ORDER BY
	date;


-- Этап 2. Создание представлений и написание аналитических запросов
-- Запрос 1. Создаем представление с данными о топ-3 заведениях каждого типа по убываюнию ср. чека
CREATE OR REPLACE VIEW v_top_3 AS
SELECT
	q2.rn AS cafe_name,
	q2.ct AS cafe_type,
	ROUND(q2.a, 2) AS average_bill
FROM
(
	-- создаем нумерацию строк по типу заведения по убыванию ср. чека
	SELECT
		q.restaurant_name rn,
		q.cafe_type ct,
	   	q.avg a,
		ROW_NUMBER() OVER(PARTITION BY q.cafe_type ORDER BY q.avg DESC) AS row 
	FROM
	(
		-- вычисляем ср. чека по заведению и его типу
		SELECT
			restaurant_name,
		   	cafe_type,
		   	AVG(cs.avg_check) AS avg
		FROM
			cafe.restaurants cr
		JOIN
			cafe.sales cs ON cs.restaurant_uuid = cr.restaurant_uuid
		GROUP BY
			restaurant_name,
			cafe_type
	) AS q
) AS q2
WHERE
	-- оставляем только топ-3 заведения каждого типа по ср. чеку в итоговом выводе
	q2.row < 4;
	
-- Запрос 2. Создаем материализованное представление с динамикой ср. чека по годам, за исключением 2023 года
CREATE MATERIALIZED VIEW mv_check_variance AS
SELECT
	q.year AS "Год",
	q.restaurant_name AS "Название заведения",
	q.cafe_type AS "Тип заведения",
	q.average_check_curr_year AS "Средний чек в этом году",
	COALESCE(q.avg_check_prev_year, 0) AS "Средний чек в предыдущем году",
	COALESCe(ROUND((q.average_check_curr_year / q.avg_check_prev_year) - 1, 2), 0.00) AS "Изменение среднего чека в %"
FROM
(
	SELECT
		EXTRACT(YEAR FROM cs.date) AS year,
	   	cr.restaurant_name AS restaurant_name,
	   	cr.cafe_type AS cafe_type,
	   	ROUND(AVG(cs.avg_check), 2) AS average_check_curr_year,
	   	LAG(ROUND(AVG(cs.avg_check), 2), 1, NULL) OVER(PARTITION BY cr.restaurant_name ORDER BY EXTRACT(YEAR FROM cs.date)) AS avg_check_prev_year
	FROM
		cafe.sales cs
	JOIN
		cafe.restaurants cr ON cs.restaurant_uuid = cr.restaurant_uuid
	WHERE
		EXTRACT(YEAR FROM cs.date) != 2023 
	GROUP BY
		year,
		restaurant_name,
		cafe_type
	ORDER BY 
		restaurant_name,
		cafe_type,
		year
) AS q;

/*
REFRESH MATERIALIZED VIEW mv_check_variance;
SELECT * FROM mv_check_variance;
*/

-- Запрос 3. Выводим топ-3 заведения по названию в которых чаще всего менялся менеджер за все время
SELECT 
	cr.restaurant_name AS restaurant_name,
	-- счетчик кол-ва уникальных менеджеров в заведении
	COUNT(DISTINCT rmwd.manager_uuid) AS counter
FROM
	cafe.restaurants cr
JOIN
	cafe.restaurant_manager_work_dates rmwd ON rmwd.restaurant_uuid = cr.restaurant_uuid
GROUP BY
	cr.restaurant_name
ORDER BY
	counter DESC
LIMIT 3;

-- Запрос 4. Выводим все заведения с наибольшим кол-вом позиций в меню. 
-- Для практики написания подзапросов в запросе отсутствуют CTE.
SELECT
	qqq.restaurant_name AS "Название заведения",
	qqq.pc AS "Кол-во пицц в меню"
FROM 
(
	-- присваиваем ранк каждому заведению по убываюнию кол-ва пицц в меню
	SELECT
		qq.restaurant_name AS restaurant_name,
	   	qq.pizza_counter AS pc,
	   	DENSE_RANK() OVER (ORDER BY qq.pizza_counter DESC) AS pizza_counter_rank
	FROM
	(
		-- считаем кол-во позиций в меню по заведениям
		SELECT
			q.restaurant_name AS restaurant_name,
	    	COUNT(DISTINCT q.pizza) AS pizza_counter
		FROM
		(
			-- выделяем заведениям и название позиции из меню по строчно только заведений с типом "pizzeria"
			SELECT
				restaurant_name,
				-- парсинг json массив на отделение названия пиццы
	   			RTRIM(LTRIM(SPLIT_PART(json_each_text(menu -> 'Пицца')::text, ',', 1), '(""'), '""') AS pizza
			FROM
				cafe.restaurants
			WHERE
				cafe_type = 'pizzeria'
		) AS q
		GROUP BY 
			restaurant_name
	) AS qq
) AS qqq
WHERE
	-- Выводим только пиццерии с наибольшим кол-вом позиций в меню
	qqq.pizza_counter_rank = 1;

-- Запрос 4. Второй вариант через key-value без парсинга
SELECT
	qqq.restaurant_name AS "Название заведения",
	qqq.pizza_counter AS "Кол-во пицц в меню"
FROM
(
	SELECT
		qq.restaurant_name AS restaurant_name,
	   	qq.pizza_counter AS pizza_counter,
	   	DENSE_RANK() OVER (ORDER BY qq.pizza_counter DESC) AS pizza_counter_rank
	FROM
	(
		SELECT
			q.restaurant_name AS restaurant_name,
	    	COUNT(DISTINCT q.pizza) AS pizza_counter
		FROM
		(
		SELECT
			restaurant_name,
	   		json_each_text(menu -> 'Пицца') AS pizza 
		FROM
			cafe.restaurants               
		WHERE
			cafe_type = 'pizzeria'
		) AS q
		GROUP BY 
			restaurant_name
	 ) AS qq
) AS qqq
WHERE
	qqq.pizza_counter_rank = 1;						

-- Запрос 5. Поиск самой дорогой пиццы для каждого заведения
SELECT
	qq.restaurant_name AS "Название заведения",
	'Пицца' AS "Тип блюда",
	qq.pizza_name AS "Название пиццы",
	qq.pizza_max_price AS "Цена"
FROM
(
	SELECT
		q.restaurant_name AS restaurant_name,
	 	q.pizza_name AS pizza_name,
	   	q.pizza_price,
	   	MAX(q.pizza_price) OVER (PARTITION BY q.restaurant_name) AS pizza_max_price
	FROM
	(
		SELECT
			restaurant_name,
	   		RTRIM(LTRIM(SPLIT_PART(json_each_text(menu -> 'Пицца')::text, ',', 1), '(""'), '"') AS pizza_name,
	   		RTRIM(LTRIM(SPLIT_PART(json_each_text(menu -> 'Пицца')::text, ',', 2), '('), ')') AS pizza_price
		FROM
			cafe.restaurants
		WHERE
			cafe_type = 'pizzeria'
	) AS q
) AS qq
WHERE
	qq.pizza_price = qq.pizza_max_price
ORDER BY
	qq.pizza_max_price DESC;

-- Запрос 6. Поиск два самых близких друг к другу заведения одного типа
SELECT DISTINCT 
	q.n1 AS "Заведение 1",
	q.n2 AS "Заведение 2",
	q.ct1 AS "Тип заведения",
	q.dist AS "Расстояние"
FROM
(
	SELECT
		cr1.restaurant_name AS n1,
	    cr2.restaurant_name AS n2,
	    cr1.cafe_type AS ct1,
	    cr2.cafe_type AS ct2,
		-- вычисляем расстояние между двумя заведениями
	    ST_Distance(cr1.restaurant_location::geography, cr2.restaurant_location::geography)::int AS dist
	FROM
		-- декартово произведение таблицы для отбора всех возможных вариантов
		cafe.restaurants cr1, cafe.restaurants cr2
) AS q
WHERE
	-- исключаем вывод одного и того же заведения, т.к. расстояние между ними будет равно 0
	q.n1 != q.n2 AND
	-- оставляем только заведения одного типа
	q.ct1 = q.ct2 
ORDER BY
	-- фильтруем по увеличению расстояния между заведениями для отбора одного
	q.dist
LIMIT 1;

-- Запрос 7. Выводим район с самым большим количеством заведений и самым маленьким
WITH
-- Подзапрос с счетчиком количества заведений расположенных в пределах одного района
w_location AS (
SELECT
	cd.district_name AS district_name,
	COUNT(cr.restaurant_uuid) AS rest_counter
FROM
	cafe.restaurants cr
JOIN
	cafe.districts cd ON ST_Within(cr.restaurant_location, cd.district_geom)
GROUP BY
	cd.district_name
)
-- находим район с наибольшим количеством заведений
SELECT
	district_name AS district,
	rest_counter
FROM
	w_location
WHERE
	rest_counter IN (SELECT MAX(rest_counter) FROM w_location)
-- присоединяем снизу районе с наименьшим количеством
UNION ALL
SELECT
	district_name AS district,
	rest_counter
FROM
	w_location
WHERE
	rest_counter IN (SELECT MIN(rest_counter) FROM w_location);

/*
DROP TYPE IF EXISTS cafe.restaurant_type CASCADE;
DROP TABLE IF EXISTS cafe.restaurants CASCADE;
DROP TABLE IF EXISTS cafe.managers CASCADE;
DROP TABLE IF EXISTS cafe.restaurant_manager_work_dates CASCADE;
DROP TABLE IF EXISTS cafe.sales CASCADE;
*/