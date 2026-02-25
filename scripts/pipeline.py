"""
pipeline.py — PyFlink Ecommerce Event Processing
- Event time processing with watermarks
- Tumbling window aggregation (clicks per user per minute)
- Anomaly detection (users with > 20 clicks/min)
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

KAFKA_BROKER = "kafka:29092"

def main():
    # ── Environment ──────────────────────────────────────────────────────────
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    settings = EnvironmentSettings.in_streaming_mode()
    tenv = StreamTableEnvironment.create(env, settings)

    # ── Source Table ─────────────────────────────────────────────────────────
    # timestamp field comes from producer: time.time() → DOUBLE seconds
    # We cast it to TIMESTAMP for event-time watermarking
    tenv.execute_sql(f"""
        CREATE TABLE ecommerce_events (
            user_id     INT,
            event       STRING,
            product_id  INT,
            `timestamp` DOUBLE,
            event_time  AS TO_TIMESTAMP_LTZ(CAST(`timestamp` * 1000 AS BIGINT), 3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector'                     = 'kafka',
            'topic'                         = 'ecommerce-events',
            'properties.bootstrap.servers'  = '{KAFKA_BROKER}',
            'properties.group.id'           = 'flink-ecommerce-group',
            'scan.startup.mode'             = 'latest-offset',
            'format'                        = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    # ── Sink: windowed aggregations ──────────────────────────────────────────
    tenv.execute_sql(f"""
        CREATE TABLE clicks_per_user (
            user_id     INT,
            window_start TIMESTAMP(3),
            window_end   TIMESTAMP(3),
            click_count  BIGINT,
            unique_products BIGINT
        ) WITH (
            'connector'                     = 'kafka',
            'topic'                         = 'ecommerce-aggregated',
            'properties.bootstrap.servers'  = '{KAFKA_BROKER}',
            'format'                        = 'json'
        )
    """)

    # ── Sink: anomalies ───────────────────────────────────────────────────────
    tenv.execute_sql(f"""
        CREATE TABLE anomalies (
            user_id      INT,
            window_start TIMESTAMP(3),
            window_end   TIMESTAMP(3),
            click_count  BIGINT,
            reason       STRING
        ) WITH (
            'connector'                     = 'kafka',
            'topic'                         = 'ecommerce-anomalies',
            'properties.bootstrap.servers'  = '{KAFKA_BROKER}',
            'format'                        = 'json'
        )
    """)

    # ── Aggregation: clicks per user per 1-minute tumbling window ─────────────
    agg_table = tenv.sql_query("""
        SELECT
            user_id,
            TUMBLE_START(event_time, INTERVAL '1' MINUTE)  AS window_start,
            TUMBLE_END(event_time,   INTERVAL '1' MINUTE)  AS window_end,
            COUNT(*)                                        AS click_count,
            COUNT(DISTINCT product_id)                      AS unique_products
        FROM ecommerce_events
        WHERE event = 'click'
        GROUP BY
            user_id,
            TUMBLE(event_time, INTERVAL '1' MINUTE)
    """)

    # Write aggregations to Kafka
    agg_table.execute_insert("clicks_per_user")

    # ── Anomaly detection: users with > 20 clicks in a 1-min window ───────────
    anomaly_table = tenv.sql_query("""
        SELECT
            user_id,
            window_start,
            window_end,
            click_count,
            CONCAT('High click rate: ', CAST(click_count AS STRING), ' clicks/min') AS reason
        FROM (
            SELECT
                user_id,
                TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
                TUMBLE_END(event_time,   INTERVAL '1' MINUTE) AS window_end,
                COUNT(*) AS click_count
            FROM ecommerce_events
            WHERE event = 'click'
            GROUP BY
                user_id,
                TUMBLE(event_time, INTERVAL '1' MINUTE)
        )
        WHERE click_count > 50
    """)

    # Write anomalies to Kafka — this call blocks and runs the job
    anomaly_table.execute_insert("anomalies").wait()


if __name__ == "__main__":
    main()