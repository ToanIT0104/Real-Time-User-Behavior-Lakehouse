from pyspark.sql.types import *

event_schema = StructType([
    StructField("api_version", StringType(), True),
    StructField("cart_products", ArrayType(
        StructType([
            StructField("amount", IntegerType(), True),
            StructField("currency", StringType(), True),
            StructField("option", ArrayType(
                StructType([
                    StructField("option_id", IntegerType(), True),
                    StructField("option_label", StringType(), True),
                    StructField("value_id", IntegerType(), True),
                    StructField("value_label", StringType(), True)
                ])
            ), True),
            StructField("price", StringType(), True),
            StructField("product_id", IntegerType(), True)
        ])
    ), True),
    StructField("cat_id", StringType(), True),
    StructField("collect_id", StringType(), True),
    StructField("collection", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("current_url", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("email_address", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("is_paypal", BooleanType(), True),
    StructField("key_search", StringType(), True),
    StructField("local_time", StringType(), True),
    StructField("option", ArrayType(
        StructType([
            StructField("Kollektion", StringType(), True),
            StructField("alloy", StringType(), True),
            StructField("category id", StringType(), True),
            StructField("diamond", StringType(), True),
            StructField("finish", StringType(), True),
            StructField("kollektion_id", StringType(), True),
            StructField("option_id", StringType(), True),
            StructField("option_label", StringType(), True),
            StructField("pearlcolor", StringType(), True),
            StructField("price", StringType(), True),
            StructField("quality", StringType(), True),
            StructField("quality_label", StringType(), True),
            StructField("shapediamond", StringType(), True),
            StructField("stone", StringType(), True),
            StructField("value_id", StringType(), True),
            StructField("value_label", StringType(), True)
        ])
    ), True),
    StructField("order_id", IntegerType(), True),
    StructField("price", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("recommendation", BooleanType(), True),
    StructField("recommendation_clicked_position", IntegerType(), True),
    StructField("recommendation_product_id", StringType(), True),
    StructField("recommendation_product_position", StringType(), True),
    StructField("referrer_url", StringType(), True),
    StructField("resolution", StringType(), True),
    StructField("show_recommendation", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("time_stamp", IntegerType(), True),
    StructField("user_agent", StringType(), True),
    StructField("user_id_db", StringType(), True),
    StructField("utm_medium", BooleanType(), True),
    StructField("utm_source", BooleanType(), True),
    StructField("viewing_product_id", StringType(), True)
])
