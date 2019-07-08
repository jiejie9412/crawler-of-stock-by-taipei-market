# Copyright 2019 LI,JIE-YING. All rights reserved.
import psycopg2
from app import config


class PostgresConnector:

    project_database_connector = psycopg2.connect(
        host=config.project_database_host,
        port=config.project_database_port,
        database=config.project_database_name,
        user=config.project_database_user,
        password=config.project_database_password,
    )

    daily_price_stock_database_connector = psycopg2.connect(
        host=config.daily_price_stock_database_host,
        port=config.daily_price_stock_database_port,
        database=config.daily_price_stock_database_name,
        user=config.daily_price_stock_database_user,
        password=config.daily_price_stock_database_password,
    )
