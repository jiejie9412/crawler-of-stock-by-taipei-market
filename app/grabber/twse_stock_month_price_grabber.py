# Copyright 2019 LI,JIE-YING. All rights reserved.
import json
import urllib.request
import random


def _date_transform_to_url_needed_string(year, month):
    if month < 10:
        month = "0" + str(month)
    return "{}{}01".format(year, month)


def _response_data_check(json_data, date_string, stock_number):
    if json_data['date'] != date_string:
        raise ValueError
    # title 原始文字："102年02月 2330 台積電           各日成交資訊"
    if json_data['title'].split(" ")[1] != stock_number:
        raise ValueError
    return json_data


class TwseStockMonthPriceGrabber(object):

    _twse_get_month_price_data_url_template = "http://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={}&stockNo={}"

    _luminati_proxy_username = 'lum-customer-hl_6e25b065-zone-static'
    _luminati_proxy_password = 'm1mzjzqnp2ip'
    _luminati_proxy_port = 22225
    _session_id = 0
    _session_id_used_counter = 99999

    def grab_stock_price(self, year, month, stock_number, use_luminati_proxy=False):
        date_string = _date_transform_to_url_needed_string(year, month)
        response_data = self._price_download_from_twse(date_string, stock_number, use_luminati_proxy)
        checked_data = _response_data_check(response_data, date_string, stock_number)
        price_data = checked_data['data']
        return price_data

    def _price_download_from_twse(self, date_string, stock_number, use_luminati_proxy):
        request_url = self._twse_get_month_price_data_url_template.format(date_string, stock_number)
        opener = self._opener_factory(use_luminati_proxy)
        content = opener.open(request_url).read().decode("utf-8")
        return json.loads(content)

    def _opener_factory(self, use_luminati_proxy):
        if use_luminati_proxy:
            opener = self._luminati_proxy_opener_factory()
        else:
            opener = urllib.request.build_opener()
        return opener

    def _luminati_proxy_opener_factory(self):
        session_id = self._random_session_id_control()
        super_proxy_url = ('http://%s-country-cn-session-%s:%s@zproxy.lum-superproxy.io:%d' % (
            self._luminati_proxy_username,
            session_id,
            self._luminati_proxy_password,
            self._luminati_proxy_port
        ))
        proxy_handler = urllib.request.ProxyHandler({
            'http': super_proxy_url,
            'https': super_proxy_url,
        })
        return urllib.request.build_opener(proxy_handler)

    def _random_session_id_control(self):
        if self._session_id_used_counter > 20:
            self._session_id_used_counter = 0
            self._session_id = random.random()
        self._session_id_used_counter += 1
        return self._session_id


if __name__ == "__main__":
    data = TwseStockMonthPriceGrabber().grab_stock_price(2018, 6, "2330", use_luminati_proxy=False)
    for i in data:
        print(i)
