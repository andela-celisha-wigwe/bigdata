import scrapy

class Blog:

	def __init__(self, name, url, archive_url_selector, comment_url_selector, comment_content_selector):
		self.name = name
		self.url = url
		self.archive_url_selector = archive_url_selector
		self.comment_url_selector = comment_url_selector
		self.comment_content_selector = comment_content_selector


class CommentSpider(scrapy.Spider):
	"""docstring for CommentSpider"""

	name = "commentspider"
	blogs = [Blog('randomest', 'http://randomest.blogspot.com.ng', 'ul.archive-list > li > a ::attr(href)', '.comment-link ::attr(href)', '#comments-block > dd > p:first-child')]
	start_urls = [ b.url for b in blogs ]
	blogs_by_urls = {b.url: b for b in blogs}

	def parse_comment_content(self, response, blog):
		# This calls the comment conent for each comment link.
		# and yields the comment body to scrapy.
		# yield the content of the comments
		for post_comment_content in response.css(blog.comment_content_selector):
			print ("\n")
			yield { 'post_comment_content': post_comment_content.css('::text').extract_first()}

	def parse_comment_link(self, response, blog):
		# This tries to get the comment links for each archive link.
		# For each of them that is available, this also tries to get the comment content or the comment body
		# by calling the parse_comment_content method of this class
		comment_links = response.css(blog.comment_url_selector).extract()
		for next_link in comment_links:
			# yield scrapy.Request(response.urljoin(next_link), callback=self.parse_comment_content)
			yield scrapy.Request(response.urljoin(next_link), callback=lambda r: self.parse_comment_content(r, blog))

	def parse(self, response):
		# this is the first method to call, It is the one that scrapy is loookng for.
		# This looks for the archive links on any page.
		# if there is no other one, the do nothing.
		# For each archive link, get the comment link(s) bu triggering the parse_comment_link method.
		
		# for post_title in response.css('.post-title'):
		# 	yield { 'post_title': post_title.css('::text').extract_first()}

		# next_page = response.css('ul.archive-list > li:nth-child({}) > a ::attr(href)'.format(self.numx)).extract_first()
		
		blog = self.blogs_by_urls[response.url]
		page_list = response.css(blog.archive_url_selector).extract()
		for next_page in page_list:
			# yield scrapy.Request(response.urljoin(next_page), callback=self.parse_comment_link)
			yield scrapy.Request(response.urljoin(next_page), callback=lambda r: self.parse_comment_link(r, blog))