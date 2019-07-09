from app.sql_connection.postgres_connector import PostgresConnector
from app.one_stock_grabbing_controller import OneStockGrabbingController
from multiprocessing import Value, Process
import time


def one_stock_launcher(stock_number, process_count_control):
    with process_count_control.get_lock():
        process_count_control.value += 1
    one_stock_grabbing_controller = OneStockGrabbingController()
    one_stock_grabbing_controller.grab_history_price(stock_number, grab_interval_sleep=False, use_luminati_proxy=True)
    with process_count_control.get_lock():
        process_count_control.value -= 1


class SystematicDailyStockPriceGrabberByTwseByMultiThread:

    def __init__(self):
        self._db_connector = PostgresConnector()
        self._init_project_database_connector()

    def _init_project_database_connector(self):
        self._project_database_connector = self._db_connector.project_database_connector
        self._project_database_cursor = self._project_database_connector.cursor()

    def grab_history_price(self):

        list_of_need_grab_history_stock_price = self._get_need_grab_history_stock_list()

        print("company_basic_information table 裡要測試的股票總數：" + str(len(list_of_need_grab_history_stock_price)))

        process_count_control = Value('i', 1)
        i = 0
        e = len(list_of_need_grab_history_stock_price)
        while i < e:
            with process_count_control.get_lock():
                if process_count_control.value < 6:
                    p = Process(target=one_stock_launcher, args=(list_of_need_grab_history_stock_price[i][1], process_count_control))
                    p.start()
                    time.sleep(3)
                    i += 1
                    print("process_count = " + str(process_count_control.value))

    def _get_need_grab_history_stock_list(self):
        select_sql = "SELECT serial_number, stock_number, status from company_basic_information WHERE status = 'b' ORDER BY serial_number;"
        self._project_database_cursor.execute(select_sql)
        need_grab_history_stock_list = self._project_database_cursor.fetchall()
        return need_grab_history_stock_list


if __name__ == "__main__":
    cla = SystematicDailyStockPriceGrabberByTwseByMultiThread()
    cla.grab_history_price()
