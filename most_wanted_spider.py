from urllib.parse import urljoin

import constants
from most_wanted_mongo import MWMongo
from base_spider import BaseSpider, get_first, get_from_trees


class FormatPipeline:
    def process_item(self, item, spider):
        suspect_descr = item.pop('suspect_descriptions', {})
        suspect_descr_formatted = {}
        for k, v in suspect_descr.items():
            if v is not None:
                suspect_descr_formatted[k.strip(':').strip()] = v.strip()

        for k, v in item.items():
            if v is not None:
                v = v.strip()
                if v.lower() in ('n/a', 'unknown', ''):
                    v = None
            item[k] = v

        item['suspect_descriptions'] = suspect_descr_formatted

        return item


class Spider(BaseSpider):

    urls = ['https://crimestoppers-uk.org/give-information/most-wanted']

    def parse(self, response, meta):
        appeal_links = response.html.xpath('//a[@class="tag text-red"]/@href')
        for appeal_link in appeal_links:
            page = urljoin(response.html.base_url, appeal_link)
            yield self.follow(page, callback=self.parse_appeal)
            # more specific requests like POST method could be done like this
            # yield FollowPage(requests.Request('POST', page), callback, meta=None)

        next_page_links = response.html.xpath('//a[@class="page-link"]/@href')
        for next_page in next_page_links:
            page = urljoin(response.html.base_url, next_page)
            yield self.follow(page, callback=self.parse)

    def parse_appeal(self, response, meta):
        item = {}

        item['URLprofile'] = response.url
        item['URLphoto'] = get_first(response.html.xpath('//div[@class="boxshadow"]//img/@src'))

        slcrs = response.html.xpath('//div[@class="col-md-8"]/ul/li')
        item[constants.crime_type] = get_from_trees(slcrs, './/*[.="Crime type:"]/following-sibling::text()[1]')
        item[constants.crime_location] = get_from_trees(slcrs, './/*[.="Crime location:"]/following-sibling::text()[1]')
        item[constants.suspect_name] = get_from_trees(slcrs, './/*[.="Suspect name:"]/following-sibling::text()[1]')
        item['nickname'] = get_from_trees(slcrs, './/*[.="Nickname:"]/following-sibling::text()[1]')
        item['number_of_people_involved'] = get_from_trees(slcrs, './/*[.="Number of people involved::"]/following-sibling::text()[1]')
        item['CS_reference'] = get_from_trees(slcrs, './/*[.="CS reference:"]/following-sibling::text()[1]')
        item['police_force'] = get_from_trees(slcrs, './/*[.="Police force"]/following-sibling::text()[1]')

        item[constants.summary] = get_first(response.html.xpath('//h2[.="Summary"]/following-sibling::text()[1]'))
        item['full_details'] = get_first(response.html.xpath('//h2[.="Full Details"]/following-sibling::text()[1]'))

        suspect_descriptions = {}
        selectors = response.html.xpath('//h2[.="Suspect description"]/following-sibling::ul/li')
        for s in selectors:
            key = get_first(s.xpath('.//strong/text()[1]'))
            if not key:
                continue
            value = get_first(s.xpath('.//text()[2]'))
            suspect_descriptions[key] = value

        item['suspect_descriptions'] = suspect_descriptions

        yield item


if __name__ == '__main__':

    mongo_pipeline = MWMongo(
        host=constants.MONGO_HOST,
        port=constants.MONGO_PORT,
        db_name=constants.MONGO_DB)

    format_pipeline = FormatPipeline()

    most_wanted = Spider(pipelines=[format_pipeline, mongo_pipeline])
    most_wanted.run()
