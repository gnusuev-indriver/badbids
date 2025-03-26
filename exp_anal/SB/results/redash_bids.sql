WITH incity AS (
            SELECT
                city_id                                                                     AS city_id,
                order_type                                                                  AS order_type,
                order_uuid                                                                  AS order_uuid,
                user_id                                                                     AS user_id,
                order_timestamp                                                             AS local_order_dttm,
                TIMESTAMP(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_timestamp), timezone) AS utc_order_dttm,
                price_highrate_usd                                                          AS price_highrate_usd,
                price_start_usd                                                             AS price_start_usd,
                price_order_usd                                                             AS price_order_usd,
                tender_uuid                                                                 AS tender_uuid,
                driver_id                                                                   AS driver_id,
                price_tender_usd                                                            AS price_tender_usd,
                driveraccept_timestamp IS NOT NULL                                          AS is_order_accepted,
                driverdone_timestamp IS NOT NULL                                            AS is_order_done,
                tender_uuid IS NOT NULL                                                     AS is_order_with_tender,
                price_start_usd = price_tender_usd                                          AS is_order_start_price_bid,
                ROW_NUMBER() OVER (PARTITION BY order_uuid ORDER BY tender_timestamp ASC)   AS first_row_by_tender,
                fromlatitude                                                                AS fromlatitude,
                fromlongitude                                                               AS fromlongitude,
                duration_in_seconds / 60                                                    AS duration_in_min,
                distance_in_meters / 1000                                                   AS distance_in_km
            FROM `indriver-e6e40.emart.incity_detail`
            WHERE true
                AND created_date_order_part >= DATE_SUB(DATE('{start_date}'), INTERVAL 1 DAY)
                AND created_date_order_part <= DATE_ADD(DATE('{stop_date}'), INTERVAL 1 DAY)
                AND city_id = {city_id}
        ) _
        WHERE DATE(utc_order_dttm) BETWEEN DATE('{start_date}') AND DATE('{stop_date}')
    ),

bid_data AS (
    SELECT DISTINCT 
        uuid AS tender_uuid,
        order_uuid                                                       AS order_uuid,
        price                                                            AS price,
        modified_at                                                      AS modified_at_utc,
        SAFE_CAST(SUBSTR(eta, 1, STRPOS(eta, 's') - 1) AS INT64)         AS eta,
        available_prices AS available_prices,
        COALESCE(
                                          (SELECT CASE
                                                      WHEN price = last_pass_price
                                                          THEN 'startprice'
                                                      WHEN price = available_prices[offset]
                                                          THEN 'option ' || CAST(offset + 1 AS STRING)
                                                      WHEN price < available_prices[SAFE_OFFSET(0)]
                                                          THEN 'option 1-'
                                                      WHEN price >
                                                           available_prices[SAFE_OFFSET(ARRAY_LENGTH(available_prices) - 1)]
                                                          THEN 'option ' || CAST(ARRAY_LENGTH(available_prices) AS STRING) || '+'
                                                      ELSE 'option ' || CAST(offset + 1 AS STRING) ||
                                                           (CASE WHEN price > available_prices[offset] THEN '+' ELSE '-' END)
                                                      END
                                           FROM UNNEST(available_prices) WITH OFFSET AS offset
                                           WHERE price = available_prices[offset]
                                              OR (price < available_prices[offset] AND
                                                  (offset = 0 OR price > available_prices[offset - 1]))
                                              OR (price > available_prices[offset] AND
                                                  (offset = ARRAY_LENGTH(available_prices) - 1 OR
                                                   price < available_prices[offset + 1]))
                                           LIMIT 1),
                                          'other'
                                  )  AS option_number
     FROM `indriver-e6e40.ods_new_order_rh_cdc.bid_global_strm`
                  WHERE true
                    AND order_uuid IS NOT NULL
                    AND uuid IS NOT NULL
                    AND order_uuid IN (SELECT order_uuid FROM incity)
                    AND status = 'BID_STATUS_ACTIVE'
                    AND DATE(created_at) BETWEEN
                      DATE_SUB('{start_date}', INTERVAL 1 DAY)
                      AND DATE_ADD('{stop_date}', INTERVAL 1 DAY)
),

pass_groups AS (
    SELECT 
        order_uuid,
        price,
        modified_at_utc,
        price_type,
        price_highrate_value,
        city_id,
        eta,
        duration_in_seconds,
    FROM (
        SELECT *
        FROM rider_data
        UNION ALL
        SELECT *
        FROM bid_data
    )
    ORDER BY order_uuid, modified_at_utc
),

full_data_table AS (
    SELECT
        price,
        price_type,
        modified_at_utc,
        eta,
        available_prices,
        LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN price END IGNORE NULLS)
                                      OVER (PARTITION BY order_uuid, pass_group ORDER BY modified_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        AS last_pass_price,
        LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN city_id END IGNORE NULLS)
               OVER (PARTITION BY order_uuid ORDER BY modified_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        AS city_id_copy,
        LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN price_highrate_value END IGNORE
               NULLS)
               OVER (PARTITION BY order_uuid ORDER BY modified_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        AS price_highrate_value,
        LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN duration_in_seconds END IGNORE
               NULLS)
               OVER (PARTITION BY order_uuid ORDER BY modified_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        AS AtoB_seconds,
    FROM pass_groups
),

options_assign AS (
    SELECT
        price_type,
        city_id_copy,
        DATETIME(TIMESTAMP(modified_at_utc), t2.timezone)                                                             AS modified_at_local,
        CASE
        WHEN price_type = 'rider_price' THEN NULL
                              ELSE
                                  COALESCE(
                                          (SELECT CASE
                                                      WHEN price = last_pass_price
                                                          THEN 'startprice'
                                                      WHEN price = available_prices[offset]
                                                          THEN 'option ' || CAST(offset + 1 AS STRING)
                                                      WHEN price < available_prices[SAFE_OFFSET(0)]
                                                          THEN 'option 1-'
                                                      WHEN price >
                                                           available_prices[SAFE_OFFSET(ARRAY_LENGTH(available_prices) - 1)]
                                                          THEN 'option ' || CAST(ARRAY_LENGTH(available_prices) AS STRING) || '+'
                                                      ELSE 'option ' || CAST(offset + 1 AS STRING) ||
                                                           (CASE WHEN price > available_prices[offset] THEN '+' ELSE '-' END)
                                                      END
                                           FROM UNNEST(available_prices) WITH OFFSET AS offset
                                           WHERE price = available_prices[offset]
                                              OR (price < available_prices[offset] AND
                                                  (offset = 0 OR price > available_prices[offset - 1]))
                                              OR (price > available_prices[offset] AND
                                                  (offset = ARRAY_LENGTH(available_prices) - 1 OR
                                                   price < available_prices[offset + 1]))
                                           LIMIT 1),
                                          'other'
                                  ) END AS option_number
    FROM full_data_table t1
    LEFT JOIN (
        SELECT 
            city_id,
            MAX(timezone) as timezone
        FROM `indriver-e6e40.emart.incity_detail`
        WHERE true
            AND created_date_order_part BETWEEN @date_range_start AND @date_range_end
        GROUP BY city_id
    ) t2
        ON t1.city_id_copy = t2.city_id
)

SELECT 
    city_id_copy,
    -- Доля бидов с завышенной ценой. Как часто завышают цену водители?
    SAFE_DIVIDE(COUNT(IF(ratio > 1.0, ratio, NULL)), COUNT(ratio))                              AS bid2rec_mph_high_bids_share,
    -- Средний завышенный бид (измерена в rec_MPH). Как сильно завышают цену водители?
    SAFE_DIVIDE(SUM(IF(ratio > 1.0, ratio, NULL)), COUNT(IF(ratio > 1.0, ratio, NULL)))         AS bid2rec_mph_high_avg
FROM options_assign
WHERE true
    AND price_type = 'bid_price'
    AND DATE(modified_at_local) BETWEEN DATE(@date_range_start) AND DATE(@date_range_end)
GROUP BY city_id_copy
ORDER BY bid2rec_mph_high_bids_share DESC, bid2rec_mph_high_avg DESC