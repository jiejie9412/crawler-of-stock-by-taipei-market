# Copyright 2019 LI,JIE-YING. All rights reserved.
from sqlalchemy import create_engine
from app import config


class PostgresConnectorBySqlalchemy:

    _connection_string_template = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    
    _project_database_connection_string = _connection_string_template.format(
            host=config.project_database_host,
            port=config.project_database_port,
            database=config.project_database_name,
            user=config.project_database_user,
            password=config.project_database_password
            )
        
    project_database_engine = create_engine(_project_database_connection_string)

    _daily_price_stock_database_connection_string = _connection_string_template.format(
            host=config.daily_price_stock_database_host,
            port=config.daily_price_stock_database_port,
            database=config.daily_price_stock_database_name,
            user=config.daily_price_stock_database_user,
            password=config.daily_price_stock_database_password
            )

    daily_price_stock_database_engine = create_engine(_daily_price_stock_database_connection_string)
