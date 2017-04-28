import re
import scrapy
# from scrapy.selector import XmlXPathSelector

base_url = "https://www.blogger.com/feeds/{}/comments/default?max-results=4000"
class RSSSpider(scrapy.Spider):

	name = 'rssspider'
	start_urls = [base_url.format(id) for id in range(9000001, 9000100)]
	comment_regex = re.compile('\<content.*\>(.+)\<\/content\>')
	published_date_regex = r'\<published.*\>(.+)\<\/published\>'
	author_regex = r"\<author.*\>.*\<name.*\>(.+)\<\/name\>"
	
	def parse(self, response):
		comment_content_list = response.selector.re(self.comment_regex)
		published_list = response.selector.re(self.published_date_regex)
		author_list = response.selector.re(self.author_regex)

		information = zip(comment_content_list, published_list, author_list)
		print(self)
		for comment_content, published_date, author in information:
			yield {
				'comment_content': comment_content,
				'published_date' : published_date,
				'author': author
			}
			# yield { 'comment_content': response.selector.re(self.comment_regex) }
		

	# def parse_node(self, response, node):
	# 	# xxs = scrapy.Selector(response)
	# 	# changed from .select to .xpath, .select is deprecated in favour of .xpath
	# 	print("\nresponse.body_as_unicode")
	# 	print(node)
	# 	# yield { 'comment_content': response.selector.xpath('//content/text()').extract_first() }
	# 	yield { 'comment_content': node.extract() }


# we'll use regex in the next class.