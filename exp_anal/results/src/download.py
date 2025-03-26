import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd

from google.cloud import bigquery
client = bigquery.Client(project='analytics-dev-333113')


def download_experiment_data(exp_id, user_name):
    tmp_query = f"""
    CREATE OR REPLACE TABLE `analytics-dev-333113.temp.{user_name}_exp`
    OPTIONS(
      expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
    ) AS (
    WITH
    exp_list AS (
        SELECT
            id                            AS exp_id,
            --name                          AS exp_name,
            CAST(REGEXP_EXTRACT(city_ids, r'\[(\d+)') AS INT64) AS city_id,
            ARRAY_LENGTH(SPLIT(TRIM(city_ids, '[]'), ',')) > 1  AS multiple_cities,
            status                        AS status, 
            start_datetime                AS utc_start_dttm,
            due_datetime                  AS utc_finish_dttm,
            salt                          AS exp_salt,
            conditions                    AS conditions,
        FROM `indriver-e6e40.ods_ab_platform.experiment`
        WHERE id = {exp_id}
    )
    SELECT DISTINCT *,
           UNIX_SECONDS(utc_start_dttm) AS utc_start_dttm_unix
    FROM exp_list
    ORDER BY exp_id, utc_start_dttm, utc_finish_dttm
    )
    """
    query = f"""
    SELECT *
    FROM `analytics-dev-333113.temp.{user_name}_exp`
    """
    client.query(tmp_query).result()
    return client.query(query).to_dataframe()


def download_order_data(start_date, stop_date, city_id, user_name):
    query = f"""
    WITH
    details_prepare AS (
        SELECT *
        FROM (
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
                distance_in_meters / 1000                                                   AS distance_in_km,
                bid_cnt                                                                     AS bid_cnt
            FROM `indriver-e6e40.emart.incity_detail`
            WHERE true
                AND created_date_order_part >= DATE_SUB(DATE('{start_date}'), INTERVAL 1 DAY)
                AND created_date_order_part <= DATE_ADD(DATE('{stop_date}'), INTERVAL 1 DAY)
                AND city_id = {city_id}
        ) _
        WHERE DATE(utc_order_dttm) BETWEEN DATE('{start_date}') AND DATE('{stop_date}')
    ),
    orders_tbl AS (
        SELECT DISTINCT
            order_uuid,
            calcprice_uuid
        FROM (
            SELECT
                uuid                                                           AS order_uuid,
                MAX(price_calculation_uuid) OVER(PARTITION BY uuid)            AS calcprice_uuid,
                ROW_NUMBER() OVER(PARTITION BY uuid ORDER BY modified_at DESC) AS rn
            FROM `indriver-e6e40.ods_new_order_rh_cdc.order_global_strm`
            WHERE true
                AND DATE(created_at) >= DATE_SUB(DATE('{start_date}'), INTERVAL 1 DAY)
                AND DATE(created_at) <= DATE_ADD(DATE('{stop_date}'), INTERVAL 1 DAY)
                AND uuid IN (SELECT order_uuid FROM details_prepare)
        ) _
        WHERE rn = 1
    ),
    details_tbl AS (
        SELECT
            t1.city_id                            AS city_id,
            t1.order_type                         AS order_type,
            t1.order_uuid                         AS order_uuid,
            t1.user_id                            AS user_id,
            t1.local_order_dttm                   AS local_order_dttm,
            t1.utc_order_dttm                     AS utc_order_dttm,
            t1.price_highrate_usd                 AS price_highrate_usd,
            t1.price_start_usd                    AS price_start_usd,
            t1.fromlatitude                       AS fromlatitude,
            t1.fromlongitude                      AS fromlongitude,
            t1.duration_in_min                    AS duration_in_min,
            t1.distance_in_km                     AS distance_in_km,
            t1.bid_cnt                            AS bid_cnt,
            t2.tenders_count                      AS tenders_count,
            t2.price_tender_usd                   AS price_tender_usd,
            t2.is_order_with_tender               AS is_order_with_tender,
            t2.is_order_start_price_bid           AS is_order_start_price_bid,
            t2.is_order_accepted_start_price_bid  AS is_order_accepted_start_price_bid,
            t2.is_order_done_start_price_bid      AS is_order_done_start_price_bid,
            t2.is_order_accepted                  AS is_order_accepted,
            t2.is_order_done                      AS is_order_done,
            t3.price_done_usd                     AS price_done_usd,
            t3.rides_price_highrate_usd           AS rides_price_highrate_usd,
            t3.rides_price_start_usd              AS rides_price_start_usd,
            t4.calcprice_uuid                     AS calcprice_uuid
        FROM details_prepare t1
        LEFT JOIN (
            SELECT
                order_uuid,
                COUNT(DISTINCT tender_uuid)                         AS tenders_count,
                AVG(price_tender_usd)                               AS price_tender_usd,
                MAX(is_order_with_tender)                           AS is_order_with_tender,
                MAX(is_order_start_price_bid)                       AS is_order_start_price_bid,
                MAX(is_order_start_price_bid AND is_order_accepted) AS is_order_accepted_start_price_bid,
                MAX(is_order_start_price_bid AND is_order_done)     AS is_order_done_start_price_bid,
                MAX(is_order_accepted)                              AS is_order_accepted,
                MAX(is_order_done)                                  AS is_order_done
            FROM details_prepare
            GROUP BY 1
        ) t2
            ON t1.order_uuid = t2.order_uuid
        LEFT JOIN (
            SELECT
                order_uuid,
                AVG(price_order_usd)    AS price_done_usd,
                AVG(price_highrate_usd) AS rides_price_highrate_usd,
                AVG(price_start_usd)    AS rides_price_start_usd
            FROM details_prepare
            WHERE is_order_done
            GROUP BY 1
        ) t3
            ON t1.order_uuid = t3.order_uuid
        LEFT JOIN orders_tbl t4
            ON t1.order_uuid = t4.order_uuid
        WHERE first_row_by_tender = 1
    )
    SELECT 
        city_id,
        order_type,
        order_uuid,
        user_id,
        local_order_dttm,
        utc_order_dttm,
        price_highrate_usd,
        price_start_usd,
        fromlatitude,
        fromlongitude,
        duration_in_min,
        distance_in_km,
        bid_cnt,
        tenders_count,
        price_tender_usd,
        is_order_with_tender,
        is_order_start_price_bid,
        is_order_accepted_start_price_bid,
        is_order_done_start_price_bid,
        is_order_accepted,
        is_order_done,
        price_done_usd,
        rides_price_highrate_usd,
        rides_price_start_usd,
        calcprice_uuid
    FROM (
            SELECT 
                t1.*, t2.* EXCEPT(city_id)
            FROM details_tbl t1
            LEFT JOIN `analytics-dev-333113.temp.{user_name}_exp` t2
                ON  t1.city_id = t2.city_id
    ) _
    """
    return client.query(query).to_dataframe()


def download_recprice_data(start_date, stop_date, city_id, user_name):
    query = f"""
    WITH
    recprice_tbl AS (
        SELECT *
        FROM (
            SELECT DISTINCT
                t1.city_id                                            AS city_id,
                t1.order_type_name                                    AS order_type,
                t1.order_type_id                                      AS order_type_id,
                t1.id                                                 AS calcprice_uuid,
                t1.user_id                                            AS user_id,
                TIMESTAMP(DATETIME(t1.calculation_dttm, t2.timezone)) AS local_recprice_dttm,
                t1.calculation_dttm                                   AS utc_recprice_dttm,
                t1.base_price / t3.usd_value                          AS price_base_usd,
                t1.price / t3.usd_value                               AS recprice_usd,
                t1.min_price / t3.usd_value                           AS minprice_usd,
                t1.surge                                              AS surge,
                t1.start_point_latitude                               AS fromlatitude,
                t1.start_point_longitude                              AS fromlongitude,
                t1.distance                                           AS log_distance_in_km,
                t1.duration / 60                                      AS log_duration_in_min
            FROM `indriver-e6e40.ods_recprice_cdc.pricing_logs` t1
            LEFT JOIN `indriver-e6e40.ods_monolith.tbl_city` t2
                ON t1.city_id = t2.id
            LEFT JOIN `indriver-bi.heap.currency_by_date` t3
                ON  t2.country_id = t3.country_id
                AND DATE(t1.calculation_dttm) = t3.date
            WHERE true
                AND DATE(t1.calculation_dttm) >= DATE_SUB(DATE('{start_date}'), INTERVAL 1 DAY)
                AND DATE(t1.calculation_dttm) <= DATE_ADD(DATE('{stop_date}'), INTERVAL 1 DAY)
                AND t1.city_id = {city_id}
                AND t1.user_id NOT IN (160705043, 10368574)
                AND (
                    t1.user_agent NOT IN ('recprice-load-generator','PHPMonolith/1','Python/3.8 aiohttp/3.8.4') OR 
                    t1.user_agent IS NULL
                )
        ) _
        WHERE DATE(utc_recprice_dttm) BETWEEN DATE('{start_date}') AND DATE('{stop_date}')
    )
    SELECT 
        city_id,
        order_type,
        order_type_id,
        calcprice_uuid,
        user_id,
        local_recprice_dttm,
        utc_recprice_dttm,
        price_base_usd,
        recprice_usd,
        minprice_usd,
        surge,
        fromlatitude,
        fromlongitude,
        log_distance_in_km,
        log_duration_in_min
    FROM (
        SELECT *
        FROM (
            SELECT 
                t1.*, t2.* EXCEPT(city_id)
            FROM recprice_tbl t1
            LEFT JOIN `analytics-dev-333113.temp.{user_name}_exp` t2
                ON  t1.city_id = t2.city_id
        ) _
    ) _
    """
    return client.query(query).to_dataframe()


def download_tender_data(start_date, stop_date, city_id, user_name):
    query = f"""
    WITH incity AS (SELECT *
                    FROM (SELECT city_id                                                                  AS city_id,
                                order_type                                                                AS order_type,
                                order_uuid                                                                AS order_uuid,
                                user_id                                                                   AS user_id,
                                order_timestamp                                                           AS local_order_dttm,
                                TIMESTAMP(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_timestamp),
                                        timezone)                                                         AS utc_order_dttm,
                                tender_uuid                                                               AS tender_uuid,
                                driver_id                                                                 AS driver_id,
                                price_highrate_usd                                                        AS price_highrate_usd,
                                price_start_usd                                                           AS price_start_usd,
                                price_order_usd                                                           AS price_order_usd,
                                price_tender_usd                                                          AS price_tender_usd,
                                driveraccept_timestamp IS NOT NULL                                        AS is_order_accepted,
                                driverdone_timestamp IS NOT NULL                                          AS is_order_done,
                                tender_uuid IS NOT NULL                                                   AS is_order_with_tender,
                                price_start_usd = price_tender_usd                                        AS is_order_start_price_bid,
                                fromlatitude                                                              AS fromlatitude,
                                fromlongitude                                                             AS fromlongitude,
                                duration_in_seconds / 60                                                  AS duration_in_min,
                                distance_in_meters / 1000                                                 AS distance_in_km,
                        FROM `indriver-e6e40.emart.incity_detail`
                        WHERE true
                            AND created_date_order_part >= DATE_SUB(DATE('{start_date}'), INTERVAL 1 DAY)
                            AND created_date_order_part <= DATE_ADD(DATE('{stop_date}'), INTERVAL 1 DAY)
                            AND city_id = {city_id})
                    WHERE DATE(utc_order_dttm) BETWEEN DATE('{start_date}') AND DATE('{stop_date}')),

        bid_data AS (SELECT DISTINCT uuid                                                     AS tender_uuid,
                                    order_uuid                                               AS order_uuid,
                                    price                                                    AS bid_price,
                                    modified_at                                              AS modified_at_utc,
                                    SAFE_CAST(SUBSTR(eta, 1, STRPOS(eta, 's') - 1) AS INT64) AS eta,
                                    available_prices                                         AS available_prices,
                                    COALESCE(
                                            (SELECT CASE
                                                     -- WHEN price = last_pass_price
                                                     --     THEN 'startprice'
                                                        WHEN price = available_prices[offset]
                                                            THEN 'option ' || CAST(offset + 1 AS STRING)
                                                        WHEN price < available_prices[SAFE_OFFSET(0)]
                                                     --     THEN 'option 1-'
                                                            THEN 'startprice'
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
                                    )                                                        AS option_number
                    FROM `indriver-e6e40.ods_new_order_rh_cdc.bid_global_strm`
                    WHERE true
                        AND order_uuid IS NOT NULL
                        AND uuid IS NOT NULL
                        AND order_uuid IN (SELECT order_uuid FROM incity)
                        AND status = 'BID_STATUS_ACTIVE'
                        AND DATE(created_at) BETWEEN
                        DATE_SUB('{start_date}', INTERVAL 1 DAY)
                        AND DATE_ADD('{stop_date}', INTERVAL 1 DAY)),

        tenders_tbl AS (SELECT incity.*,
                               bid_data.bid_price / coalesce(multiplier_data.multiplier, 100) AS bid_price_currency,
                               ARRAY(SELECT price / coalesce(multiplier_data.multiplier, 100) FROM UNNEST(bid_data.available_prices) as price) AS available_prices_currency,
                               bid_data.eta,
                               bid_data.option_number
                        FROM incity
                                LEFT JOIN bid_data
                                        ON incity.order_uuid = bid_data.order_uuid
                                            AND incity.tender_uuid = bid_data.tender_uuid
                                LEFT JOIN (SELECT city_id,
                                                  MAX(multiplier) AS multiplier
                                            FROM `indriver-e6e40.ods_new_order_rh_cdc.order_global_strm`
                                            WHERE true
                                                AND city_id IN (SELECT DISTINCT city_id FROM incity)
                                                AND DATE(created_at) >= DATE_SUB(DATE('{start_date}'), INTERVAL 1 DAY)
                                                AND DATE(created_at) <= DATE_ADD(DATE('{stop_date}'), INTERVAL 1 DAY)
                                            GROUP BY city_id) AS multiplier_data
                                        ON incity.city_id = multiplier_data.city_id
                        WHERE true)

    SELECT 
        *
    FROM tenders_tbl t1
    """
    
    return client.query(query).to_dataframe()