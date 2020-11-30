# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Item, Field


class EstateItem(Item):
    # define the fields for yourField() item here like:
    # name = scrapy.Field()
    pass


class LianJiaSecondHandItem(Item):
    collection = 'second_hand_lianjia_onsale'
    house_id = Field()
    title = Field()
    position_building = Field()
    position_area = Field()
    house_info = Field()
    follow_info = Field()
    tag_info = Field()
    total_price = Field()
    total_price_unit = Field()
    # 元/平米
    unit_price = Field()
    update_date = Field()


class LianJiaSecondHandChengjiaoItem(Item):
    collection = 'second_hand_lianjia_chengjiao'
    position_building = Field()  # 小区名称
    position_area = Field()  # 小区所属区域
    # 元/平米
    deal_unit_price = Field()  # 成交单价， 万/平米
    deal_price = Field()  # 成交价格，万
    deal_date = Field()  # 成交日期 yyyy.MM.dd
    listed_price = Field()  # 挂牌价格，万
    deal_period = Field()  # 成交周期
    # 房屋详细信息
    house_type = Field()  # 房屋户型
    floor = Field()  # 所在楼层
    size = Field()  # 建筑面积
    house_structure = Field()  # 户型结构
    inside_space = Field()  # 套内面积
    building_type = Field()  # 建筑结构
    house_aspect = Field()  # 房屋朝向
    building_year = Field()  # 建成年代
    decoration = Field()  # 装修情况
    building_structure = Field()  # 建筑结构
    heating_type = Field()  # 供暖方式
    ladder_household_proportion = Field()  # 梯户比例
    has_elevator = Field()  # 配备电梯

    # 房屋交易信息
    lianjia_id = Field()  # 链家编号
    transaction_ownership = Field()  # 交易权属
    listed_date = Field()  # 挂牌时间
    house_use = Field()  # 房屋用途
    house_used_year = Field()  # 房屋年限
    house_ownership = Field()  # 房权所属
    update_date = Field()

    def set_house_base_info(self, label, data):
        if "房屋户型" == label:
            self['house_type'] = data
        elif "所在楼层" == label:
            self['floor'] = data
        elif "建筑面积" == label:
            self['size'] = data
        elif "户型结构" == label:
            self['house_structure'] = data
        elif "套内面积" == label:
            self['inside_space'] = data
        elif "建筑结构" == label:
            self['building_type'] = data
        elif "房屋朝向" == label:
            self['house_aspect'] = data
        elif "建成年代" == label:
            self['building_year'] = data
        elif "装修情况" == label:
            self['decoration'] = data
        elif "建筑结构" == label:
            self['building_structure'] = data
        elif "供暖方式" == label:
            self['heating_type'] = data
        elif "梯户比例" == label:
            self['ladder_household_proportion'] = data
        elif "配备电梯" == label:
            self['has_elevator'] = data

    def set_house_transaction_info(self, label, data):
        if "链家编号" == label:
            self['lianjia_id'] = data
        elif "交易权属" == label:
            self['transaction_ownership'] = data
        elif "挂牌时间" == label:
            self['listed_date'] = data
        elif "房屋用途" == label:
            self['house_use'] = data
        elif "房屋年限" == label:
            self['house_used_year'] = data
        elif "房权所属" == label:
            self['house_ownership'] = data
