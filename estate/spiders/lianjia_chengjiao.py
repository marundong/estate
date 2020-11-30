import datetime
import json
import random
import re
from time import sleep

import scrapy

from estate.items import LianJiaSecondHandChengjiaoItem


class LianjiaSpider(scrapy.Spider):
    name = 'lianjia_chengjiao'
    allowed_domains = ['xa.lianjia.com']
    start_urls = ['https://xa.lianjia.com/chengjiao/']

    def parse(self, response):

        # 链家页面只返回100页数据，即3000条，需要分类查询
        if self.need_filter(response):
            filter_position = response.xpath(
                '//div[@class="m-filter"]//div[@class="position"]//div[@data-role="ershoufang"]//a/@href').getall()
            for url_position in filter_position:
                url = response.urljoin(url_position)
                yield scrapy.Request(url=url, callback=self.parse_price)
        else:
            yield from self.deal_list(response, self.parse)

    def parse_price(self, response):
        if self.need_filter(response):
            href = response.xpath('//div[@class="m-filter"]//div[@class="list-more"]/dl[1]/dd/a/@href').getall()
            for url_price in href:
                url = response.urljoin(url_price)
                yield scrapy.Request(url=url, callback=self.parse_house_type)
        else:
            yield from self.deal_list(response, self.parse_price)

    def parse_house_type(self, response):
        if self.need_filter(response):
            href = response.xpath('//div[@class="m-filter"]//div[@class="list-more"]/dl[2]/dd/a/@href').getall()
            for url_price in href:
                url = response.urljoin(url_price)
                yield scrapy.Request(url=url, callback=self.parse_area)
        else:
            yield from self.deal_list(response, self.parse_house_type)

    def parse_area(self, response):
        if self.need_filter(response):
            href = response.xpath('//div[@class="m-filter"]//div[@class="list-more"]/dl[3]/dd/a/@href').getall()
            for url_price in href:
                url = response.urljoin(url_price)
                yield scrapy.Request(url=url, callback=self.parse_area)
        else:
            yield from self.deal_list(response, self.parse_area)

    def parse_aspect(self, response):
        if self.need_filter(response):
            href = response.xpath('//div[@class="m-filter"]//div[@class="list-more"]/dl[4]/dd/a/@href').getall()
            for url_price in href:
                url = response.urljoin(url_price)
                yield scrapy.Request(url=url, callback=self.parse_floor)
        else:
            yield from self.deal_list(response, self.parse_aspect)

    def parse_floor(self, response):
        if self.need_filter(response):
            href = response.xpath('//div[@class="m-filter"]//div[@class="list-more"]/dl[5]/dd/a/@href').getall()
            for url_price in href:
                url = response.urljoin(url_price)
                yield scrapy.Request(url=url, callback=self.parse_use)
        else:
            yield from self.deal_list(response, self.parse_floor)

    def parse_use(self, response):
        if self.need_filter(response):
            href = response.xpath('//div[@class="m-filter"]//div[@class="list-more"]/dl[6]/dd/a/@href').getall()
            for url_price in href:
                url = response.urljoin(url_price)
                yield scrapy.Request(url=url, callback=self.parse_heat)
        else:
            yield from self.deal_list(response, self.parse_use)

    def parse_heat(self, response):
        if self.need_filter(response):
            href = response.xpath('//div[@class="m-filter"]//div[@class="list-more"]/dl[7]/dd/a/@href').getall()
            for url_price in href:
                url = response.urljoin(url_price)
                yield scrapy.Request(url=url, callback=self.parse_data)
        else:
            yield from self.deal_list(response, self.parse_heat)

    def parse_data(self, response):
        yield from self.deal_list(response, self.parse_heat)

    @staticmethod
    def need_filter(response):
        total = response.xpath(
            '//div[contains(@class,"content")]//div[@class="leftContent"]//div[contains(@class, "resultDes")]//h2[contains(@class, "total")]//span/text()').get()
        if not total:
            total = response.xpath(
                '//div[contains(@class,"content")]//div[@class="leftContent"]//div[contains(@class, "resultDes")]//div[contains(@class, "total")]//span/text()').get()
        if total:
            total = int(total.strip())
            return total > 3000
        else:
            return False

    def deal_list(self, response, callback):
        total = response.xpath(
            '//div[contains(@class,"content")]//div[@class="leftContent"]//div[contains(@class, "resultDes")]//h2[contains(@class, "total")]//span/text()').get()
        if not total:
            total = response.xpath(
                '//div[contains(@class,"content")]//div[@class="leftContent"]//div[contains(@class, "resultDes")]//div[contains(@class, "total")]//span/text()').get()
        if not total or int(total.strip()) == 0:
            return

        detail_url_list = response.xpath('//div[contains(@class,"content")]//div[@class="leftContent"]//ul[@class="listContent"]//li//div[@class="title"]/a/@href').getall()
        for detail_url in detail_url_list:
            yield scrapy.Request(url=detail_url, callback=self.deal_detail)

        page_info = response.xpath('////div[contains(@class,"contentBottom")]//div[contains(@class,"house-lst-page-box")]')
        page_url = page_info.xpath('@page-url').extract_first()
        page_data = json.loads(page_info.xpath('@page-data').extract_first())
        total_page = page_data.get('totalPage')
        next_page = page_data.get('curPage') + 1
        sleep(random.randint(1, 5))
        if next_page <= total_page:
            url = page_url.format(page=str(next_page))
            url = response.urljoin(url)
            yield scrapy.Request(url=url, callback=callback)

    def deal_detail(self, response):
        item = LianJiaSecondHandChengjiaoItem()
        # 房屋标题，形如：浩林方里 2室1厅 68.18平米
        house_title = response.xpath('//div[contains(@class,"house-title")]//h1/text()').extract_first()
        house_deal_date = response.xpath('//div[contains(@class,"house-title")]//div[@class="wrapper"]/span/text()').extract_first()
        match_obj = re.match('(.*?) ', house_title)
        if match_obj:
            # 小区名称
            item['position_building'] = match_obj.group(1)
        area = response.xpath('//section[contains(@class,"houseContentBox")]//div[@class="m-right"]//div[@class="agent-box"]//div[@class="name"]//a/text()').getall()
        item['position_area'] = '-'.join(area)
        # 成交时间
        item['deal_date'] = re.search(r"(\d{4}\.\d{1,2}\.\d{1,2})", house_deal_date).group(0)
        # 房屋基础信息
        base_li_list = response.xpath('//section[contains(@class,"houseContentBox")]//div[contains(@class,"baseinform")]//div[@class="introContent"]//div[@class="base"]//div[@class="content"]//ul//li')
        # 房屋交易信息
        transaction_li_list = response.xpath('//section[contains(@class,"houseContentBox")]//div[contains(@class,"baseinform")]//div[@class="introContent"]//div[@class="transaction"]//div[@class="content"]//ul/li')
        # 挂牌信息和成交周期
        overview_span_list = response.xpath('//section[@class="wrapper"]//div[@class="overview"]//div[contains(@class, "info")]//div[@class="msg"]//span')
        # 第一个为挂牌价格
        item['listed_price'] = float(overview_span_list[0].xpath('./label/text()').get().strip())
        # 第二个为成交周期
        item['deal_period'] = int(overview_span_list[1].xpath('./label/text()').get().strip())
        # 成交单价
        unit_price = response.xpath(
            '//section[@class="wrapper"]//div[@class="overview"]//div[contains(@class, "info")]//div[@class="price"]//b/text()').get()
        item['deal_unit_price'] = float(unit_price.strip())
        # 成交记录获取最终成交价格
        match_obj_recorde = re.match('(.*?)万', response.xpath('//section[@class="houseContentBox"]//div[@class="chengjiao_record"]//ul/li[1]/span[@class="record_price"]//text()').get())

        if match_obj_recorde:
            item['deal_price'] = float(match_obj_recorde.group(1).strip())
        for li in base_li_list:
            # 房屋基本信息
            label = li.xpath('./span[@class="label"]/text()').get()
            data = li.xpath('./text()').get()
            item.set_house_base_info(label, data.strip())
        for li in transaction_li_list:
            # 房屋交易信息
            label = li.xpath('./span[@class="label"]/text()').get()
            data = li.xpath('./text()').get()
            item.set_house_transaction_info(label, data.strip())
        yield item
