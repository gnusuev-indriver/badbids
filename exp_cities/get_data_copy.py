import pandas as pd
import warnings

def get_data(start_date = '2025-02-01', 
             stop_date = '2025-02-31',
             pickup_eta_minutes = 0,
            #  alpha = 0,
             city_id = 0,
             type_name = 'auto_econom'):
        
    new_df = pd.read_gbq(f"""
WITH rider_data AS (SELECT city_id              AS city_id,
                           timezone             AS timezone,
                           type_name            AS type_name,
                           uuid                 AS order_uuid,
                           CAST(NULL AS STRING) AS tender_uuid,
                           'rider_price'        AS price_type,
                           payment_price_value  AS price,
                           price_highrate_value AS price_highrate_value,
                           accepted_tender_uuid AS accepted_tender_uuid,
                           AtoB_seconds         AS AtoB_seconds,
                           CAST(NULL AS INT64)  AS eta,
                           modified_at          AS modified_at_utc,
                           order_done           AS order_done
                    FROM `indriver-e6e40.ods_new_order_rh_cdc.order_global_strm` t1
                             LEFT JOIN (SELECT order_uuid,
                                               MAX(timezone)                                                            AS timezone,
                                               MAX(CASE WHEN driverdone_timestamp IS NOT NULL THEN tender_uuid END)     AS accepted_tender_uuid,
                                               MAX(CASE WHEN driverdone_timestamp IS NOT NULL THEN true ELSE false END) AS order_done,
                                               MAX(duration_in_seconds)                                                 AS AtoB_seconds
                                        FROM `indriver-e6e40.emart.incity_detail`
                                        WHERE true
                                          AND order_uuid IS NOT NULL
                                          AND city_id = {city_id}
                                          AND created_date_order_part BETWEEN
                                            DATE_SUB('{start_date}', INTERVAL 1 DAY)
                                            AND DATE_ADD('{stop_date}', INTERVAL 1 DAY)
                                        GROUP BY order_uuid) t2
                                       ON t1.uuid = t2.order_uuid
                    WHERE true
                      AND order_uuid IS NOT NULL
                      AND status = 'ORDER_STATUS_ACTIVE'
                      AND AtoB_seconds > 0
                      AND type_name = {type_name}
                      AND city_id = {city_id}
                      AND DATE(created_at) BETWEEN
                        DATE_SUB('{start_date}', INTERVAL 1 DAY)
                        AND DATE_ADD('{stop_date}', INTERVAL 1 DAY)
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY uuid, payment_price_value ORDER BY modified_at) = 1),

     bid_data AS (SELECT DISTINCT CAST(NULL AS INT64)                                      AS city_id,
                                  CAST(NULL AS STRING)                                     AS timezone,
                                  CAST(NULL AS STRING)                                     AS type_name,
                                  order_uuid                                               AS order_uuid,
                                  uuid                                                     AS tender_uuid,
                                  'bid_price'                                              AS price_type,
                                  price                                                    AS price,
                                  CAST(NULL AS INT64)                                      AS price_highrate_value,
                                  CAST(NULL AS STRING)                                     AS accepted_tender_uuid,
                                  CAST(NULL AS INT64)                                      AS AtoB_seconds,
                                  SAFE_CAST(SUBSTR(eta, 1, STRPOS(eta, 's') - 1) AS INT64) AS eta,
                                  modified_at                                              AS modified_at_utc,
                                  CAST(NULL AS BOOL)                                       AS order_done,
                  FROM `indriver-e6e40.ods_new_order_rh_cdc.bid_global_strm`
                  WHERE true
                    AND order_uuid IS NOT NULL
                    AND uuid IS NOT NULL
                    AND order_uuid IN (SELECT order_uuid FROM rider_data)
                    AND status = 'BID_STATUS_ACTIVE'
                    AND DATE(created_at) BETWEEN
                      DATE_SUB('{start_date}', INTERVAL 1 DAY)
                      AND DATE_ADD('{stop_date}', INTERVAL 1 DAY)),

     my_strm AS (SELECT city_id,
                        timezone,
                        type_name,
                        order_uuid,
                        tender_uuid,
                        price_type,
                        price,
                        price_highrate_value,
                        accepted_tender_uuid,
                        AtoB_seconds,
                        eta,
                        modified_at_utc,
                        order_done
                 FROM (SELECT *
                       FROM rider_data
                       UNION ALL
                       SELECT *
                       FROM bid_data)
                 ORDER BY order_uuid, modified_at_utc),

     my_strm_full AS (SELECT LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN city_id END IGNORE NULLS)
                                        OVER w        AS city_id,
                             LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN timezone END IGNORE NULLS)
                                        OVER w        AS timezone,
                             LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN type_name END IGNORE NULLS)
                                        OVER w        AS type_name,
                             order_uuid,
                             tender_uuid,
                             price_type,
                             price,
                             LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN price_highrate_value END IGNORE
                                        NULLS) OVER w AS price_highrate_value,
                             LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN price END IGNORE
                                        NULLS) OVER w AS start_price_value,
                             LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN accepted_tender_uuid END IGNORE
                                        NULLS) OVER w AS accepted_tender_uuid,
                             LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN AtoB_seconds END IGNORE
                                        NULLS) OVER w AS AtoB_seconds,
                             eta,
                             modified_at_utc,
                             LAST_VALUE(CASE WHEN price_type = 'rider_price' THEN order_done END IGNORE
                                        NULLS) OVER w AS order_done
                      FROM my_strm
                      WINDOW w AS (
                              PARTITION BY order_uuid
                              ORDER BY modified_at_utc
                              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )),

     options_assign AS (SELECT city_id,
                               type_name,
                               order_uuid,
                               tender_uuid,
                               price_type,
                               accepted_tender_uuid,
                               order_done,
                               DATETIME(TIMESTAMP(modified_at_utc), timezone)               AS modified_at_local,
                               SAFE_DIVIDE(SAFE_DIVIDE(price, (AtoB_seconds + eta)),
                                           SAFE_DIVIDE(GREATEST(price_highrate_value, start_price_value),
                                                       (AtoB_seconds + {pickup_eta_minutes} * 60))) AS ratio
                        FROM my_strm_full)

SELECT city_id,
       type_name,
       COUNT(DISTINCT IF(order_done = true, order_uuid, NULL))       AS rides_cnt,
       {pickup_eta_minutes} as t_param,
       -- alpha = 0.0
       -- COUNT(IF(order_done = true AND tender_uuid = accepted_tender_uuid AND
       --         ratio > 1 + 0.0, price_type, NULL))              AS rides_badbid_cnt_00,
       SAFE_DIVIDE(COUNT(IF(ratio > 1 + 0.0, price_type, NULL)),
                   COUNT(price_type))                                AS badbids_share_00,
       SAFE_DIVIDE(SUM(IF(ratio > 1 + 0.0, ratio, NULL)),
                   COUNT(IF(ratio > 1 + 0.0, price_type, NULL))) AS badbid_ratio_avg_00,
       SAFE_DIVIDE(COUNT(IF(order_done = true AND tender_uuid = accepted_tender_uuid AND
                            ratio > 1 + 0.0, price_type, NULL)),
                   COUNT(DISTINCT IF(order_done = true, order_uuid, NULL)))
                                                                     AS rides_at_risk_00,
       -- alpha = 0.05
       --COUNT(IF(order_done = true AND tender_uuid = accepted_tender_uuid AND
       --         ratio > 1 + 0.05, price_type, NULL))              AS rides_badbid_cnt_05,
       SAFE_DIVIDE(COUNT(IF(ratio > 1 + 0.05, price_type, NULL)),
                   COUNT(price_type))                                AS badbids_share_05,
       SAFE_DIVIDE(SUM(IF(ratio > 1 + 0.05, ratio, NULL)),
                   COUNT(IF(ratio > 1 + 0.05, price_type, NULL))) AS badbid_ratio_avg_05,
       SAFE_DIVIDE(COUNT(IF(order_done = true AND tender_uuid = accepted_tender_uuid AND
                            ratio > 1 + 0.05, price_type, NULL)),
                   COUNT(DISTINCT IF(order_done = true, order_uuid, NULL)))
                                                                     AS rides_at_risk_05,
       -- alpha = 0.1
       --COUNT(IF(order_done = true AND tender_uuid = accepted_tender_uuid AND
       --         ratio > 1 + 0.1, price_type, NULL))              AS rides_badbid_cnt_10,
       SAFE_DIVIDE(COUNT(IF(ratio > 1 + 0.1, price_type, NULL)),
                   COUNT(price_type))                                AS badbids_share_10,
       SAFE_DIVIDE(SUM(IF(ratio > 1 + 0.1, ratio, NULL)),
                   COUNT(IF(ratio > 1 + 0.1, price_type, NULL))) AS badbid_ratio_avg_10,
       SAFE_DIVIDE(COUNT(IF(order_done = true AND tender_uuid = accepted_tender_uuid AND
                            ratio > 1 + 0.1, price_type, NULL)),
                   COUNT(DISTINCT IF(order_done = true, order_uuid, NULL)))
                                                                     AS rides_at_risk_10,
       -- alpha = 0.125
       --COUNT(IF(order_done = true AND tender_uuid = accepted_tender_uuid AND
       --         ratio > 1 + 0.125, price_type, NULL))              AS rides_badbid_cnt_125,
       SAFE_DIVIDE(COUNT(IF(ratio > 1 + 0.125, price_type, NULL)),
                   COUNT(price_type))                                AS badbids_share_125,
       SAFE_DIVIDE(SUM(IF(ratio > 1 + 0.125, ratio, NULL)),
                   COUNT(IF(ratio > 1 + 0.125, price_type, NULL))) AS badbid_ratio_avg_125,
       SAFE_DIVIDE(COUNT(IF(order_done = true AND tender_uuid = accepted_tender_uuid AND
                            ratio > 1 + 0.125, price_type, NULL)),
                   COUNT(DISTINCT IF(order_done = true, order_uuid, NULL)))
                                                                     AS rides_at_risk_125,
       -- alpha = 0.15
       --COUNT(IF(order_done = true AND tender_uuid = accepted_tender_uuid AND
       --         ratio > 1 + 0.15, price_type, NULL))              AS rides_badbid_cnt_15,
       SAFE_DIVIDE(COUNT(IF(ratio > 1 + 0.15, price_type, NULL)),
                   COUNT(price_type))                                AS badbids_share_15,
       SAFE_DIVIDE(SUM(IF(ratio > 1 + 0.15, ratio, NULL)),
                   COUNT(IF(ratio > 1 + 0.15, price_type, NULL))) AS badbid_ratio_avg_15,
       SAFE_DIVIDE(COUNT(IF(order_done = true AND tender_uuid = accepted_tender_uuid AND
                            ratio > 1 + 0.15, price_type, NULL)),
                   COUNT(DISTINCT IF(order_done = true, order_uuid, NULL)))
                                                                     AS rides_at_risk_15
FROM options_assign
WHERE true
  AND price_type = 'bid_price'
  AND DATE(modified_at_local) BETWEEN DATE('{start_date}') AND DATE('{stop_date}')
GROUP BY city_id, type_name
-- ORDER BY badbids_share DESC, badbid_ratio_avg DESC;
""")
    return new_df