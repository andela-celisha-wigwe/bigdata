import scrapy

class BaseSpider(scrapy.Spider):
	name = "basespider"
	base_url = "https://www.blogger.com/next-blog?blogID="

	# Generates an arrayh of start urls
	# E.g
	# https://www.blogger.com/next-blog?blogID=7003230
	# https://www.blogger.com/next-blog?blogID=9995647
	start_urls = [base_url + id for id in range(700000, 900000)]

	def parse(self, response):


        for title in response.css('.entry-title'):
            yield {'title': title.css('a ::text').extract_first()}

        next_page = response.css('div.prev-post > a ::attr(href)').extract_first()
        if next_page:
            yield scrapy.Request(response.urljoin(next_page), callback=self.parse)
