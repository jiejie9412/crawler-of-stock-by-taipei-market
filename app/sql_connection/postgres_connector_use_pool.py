# Copyright 2019 LI,JIE-YING. All rights reserved.
from psycopg2 import pool
from app import config


class PostgresConnectorUsePool:

    project_database_connection_pool = pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=5,
        host=config.project_database_host,
        port=config.project_database_port,
        database=config.project_database_name,
        user=config.project_database_user,
        password=config.project_database_password,
    )

    daily_price_stock_database_connection_pool = pool.ThreadedConnectionPool(
        minconn=5,
        maxconn=50,
        host=config.daily_price_stock_database_host,
        port=config.daily_price_stock_database_port,
        database=config.daily_price_stock_database_name,
        user=config.daily_price_stock_database_user,
        password=config.daily_price_stock_database_password,
    )
