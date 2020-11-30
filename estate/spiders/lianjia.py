import datetime
import json
import random
from time import sleep

import scrapy

from estate.items import LianJiaSecondHandItem


class LianjiaSpider(scrapy.Spider):
    name = 'lianjia'
    allowed_domains = ['xa.lianjia.com']
    start_urls = ['https://xa.lianjia.com/ershoufang/']

    def parse(self, response):

        # 链家页面只返回100页数据，即3000条，需要分类查询
        if self.need_filter(response):
            filter_position = response.xpath(
                '//div[@class="m-filter"]//div[@class="position"]//div[@data-role="ershoufang"]//a/@href').getall()
            for url_position in filter_position:
                url = response.urljoin(url_position)
                yield scrapy.Request(url=url, callback=self.parse_price)
        else:
            yield from self.deal_data(response, self.parse)

    def parse_price(self, response):
        if self.need_filter(response):
            href = response.xpath('//div[@class="m-filter"]//div[@class="list-more"]/dl[1]/dd/a/@href').getall()
            for url_price in href:
                url = response.urljoin(url_price)
                yield scrapy.Request(url=url, callback=self.parse_house_type)
        else:
            yield from self.deal_data(response, self.parse_price)

    def parse_house_type(self, response):
        if self.need_filter(response):
            href = response.xpath('//div[@class="m-filter"]//div[@class="list-more"]/dl[2]/dd/a/@href').getall()
            for url_price in href:
                url = response.urljoin(url_price)
                yield scrapy.Request(url=url, callback=self.parse_area)
        else:
            yield from self.deal_data(response, self.parse_house_type)

    def parse_area(self, response):
        if self.need_filter(response):
            href = response.xpath('//div[@class="m-filter"]//div[@class="list-more"]/dl[3]/dd/a/@href').getall()
            for url_price in href:
                url = response.urljoin(url_price)
                yield scrapy.Request(url=url, callback=self.parse_area)
        else:
            yield from self.deal_data(response, self.parse_area)

    def parse_aspect(self, response):
        if self.need_filter(response):
            href = response.xpath('//div[@class="m-filter"]//div[@class="list-more"]/dl[4]/dd/a/@href').getall()
            for url_price in href:
                url = response.urljoin(url_price)
                yield scrapy.Request(url=url, callback=self.parse_floor)
        else:
            yield from self.deal_data(response, self.parse_aspect)

    def parse_floor(self, response):
        if self.need_filter(response):
            href = response.xpath('//div[@class="m-filter"]//div[@class="list-more"]/dl[5]/dd/a/@href').getall()
            for url_price in href:
                url = response.urljoin(url_price)
                yield scrapy.Request(url=url, callback=self.parse_use)
        else:
            yield from self.deal_data(response, self.parse_floor)

    def parse_use(self, response):
        if self.need_filter(response):
            href = response.xpath('//div[@class="m-filter"]//div[@class="list-more"]/dl[6]/dd/a/@href').getall()
            for url_price in href:
                url = response.urljoin(url_price)
                yield scrapy.Request(url=url, callback=self.parse_heat)
        else:
            yield from self.deal_data(response, self.parse_use)

    def parse_heat(self, response):
        if self.need_filter(response):
            href = response.xpath('//div[@class="m-filter"]//div[@class="list-more"]/dl[7]/dd/a/@href').getall()
            for url_price in href:
                url = response.urljoin(url_price)
                yield scrapy.Request(url=url, callback=self.parse_data)
        else:
            yield from self.deal_data(response, self.parse_heat)

    def parse_data(self, response):
        yield from self.deal_data(response, self.parse_heat)

    @staticmethod
    def need_filter(response):
        total = response.xpath(
            '//div[contains(@class,"content")]//div[@class="leftContent"]//div[contains(@class, "resultDes")]//h2[contains(@class, "total")]//span/text()').get()
        if not total:
            total = response.xpath(
                '//div[contains(@class,"content")]//div[@class="leftContent"]//div[contains(@class, "resultDes")]//duv[contains(@class, "total")]//span/text()').get()
        if total:
            total = int(total.strip())
            return total > 3000
        else:
            return False

    @staticmethod
    def deal_data(response, callback):
        total = response.xpath(
            '//div[contains(@class,"content")]//div[@class="leftContent"]//div[contains(@class, "resultDes")]//h2[contains(@class, "total")]//span/text()').get()
        if not total:
            total = response.xpath(
                '//div[contains(@class,"content")]//div[@class="leftContent"]//div[contains(@class, "resultDes")]//div[contains(@class, "total")]//span/text()').get()
        if not total or int(total.strip()) == 0:
            return
        li_list = response.xpath(
            '//div[contains(@class,"content")]//div[@class="leftContent"]//ul[@class="sellListContent"]//li[contains(@class,"LOGCLICKDATA")]')
        for li in li_list:
            item = LianJiaSecondHandItem()
            item['house_id'] = li.xpath('./@data-lj_action_housedel_id').extract_first()
            info = li.xpath('./div[contains(@class,"info")]')
            item['title'] = info.xpath('./div[@class="title"]//a/text()').extract_first()
            item['position_building'] = info.xpath('./div[@class="flood"]//a[1]/text()').extract_first()
            item['position_area'] = info.xpath('./div[@class="flood"]//a[2]/text()').extract_first()
            item['house_info'] = info.xpath('./div[@class="address"]/div[@class="houseInfo"]/text()').extract_first()
            item['follow_info'] = info.xpath('./div[@class="followInfo"]/text()').extract_first()
            item['tag_info'] = ','.join(info.xpath('./div[@class="tag"]/span/text()').extract())
            item['total_price'] = float(
                info.xpath('./div[@class="priceInfo"]/div[@class="totalPrice"]/span/text()').extract_first())
            item['total_price_unit'] = info.xpath(
                './div[@class="priceInfo"]/div[@class="totalPrice"]/text()').extract_first()
            # 元/平米
            item['unit_price'] = float(
                info.xpath('./div[@class="priceInfo"]/div[@class="unitPrice"]/@data-price').extract_first())
            item['update_date'] = datetime.datetime.now()
            yield item
        page_info = response.xpath(
            '//div[@class="content "]//div[@class="leftContent"]//div[contains(@class,"contentBottom")]//div[contains(@class,"house-lst-page-box")]')
        page_url = page_info.xpath('@page-url').extract_first()
        page_data = json.loads(page_info.xpath('@page-data').extract_first())
        total_page = page_data.get('totalPage')
        next_page = page_data.get('curPage') + 1
        sleep(random.randint(1, 10))
        if next_page <= total_page:
            url = page_url.format(page=str(next_page))
            url = response.urljoin(url)
            yield scrapy.Request(url=url, callback=callback)
