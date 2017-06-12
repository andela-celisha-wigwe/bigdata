"""
 Go to blogspot
 grab at leat 400000 blog posts
 
 blog_id of the blog
 post_url of post
 author of post
 date_created of post
 title of post
 number of comments.
 post content
"""

import scrapy

base_url = "https://www.blogger.com/feeds/{}/posts/default?max-results=4000"

class AnotherSpider(scrapy.Spider):
	"""Another Spider"""
	name = "anotherspider"
	
	# 7000049
	start_urls = [base_url.format(id) for id in range(7007001, 7008000)]

	blog_id_regex = r'\<id>tag:blogger.com,\d{1,}:blog-(\d*)<\/id>'
	author_regex = r"\<author.*\>.*\<name.*\>(.+)\<\/name\>"
	title_regex = r'\<title.*\>(.+)\<\/title\>'
	date_created_regex = r'\<published.*\>(.+)\<\/published\>'
	comment_count_regex = r'\<link rel="replies" type="text\/html" .* title="(\d{1,}) Comments"\/>'
	post_url_regex = r'\<link rel="alternate" type="text\/html" href="([^"]*)".*\/>'
	post_content_regex = r'\<content.*\>(.+)\<\/content\>'

	def parse(self, response):
		blog_id_list = response.selector.re(self.blog_id_regex)
		author_list = response.selector.re(self.author_regex)
		title_list = response.selector.re(self.title_regex)
		date_created_list = response.selector.re(self.date_created_regex)
		comment_count_list = response.selector.re(self.comment_count_regex)
		post_url_list = response.selector.re(self.post_url_regex)
		post_content_list = response.selector.re(self.post_content_regex)

		info = zip(blog_id_list, author_list, title_list, date_created_list, comment_count_list, post_url_list, post_content_list)
		for blog_id, author, title, date_created, comment_count, post_url, post_content in info:
			yield {
				'id': blog_id,
				'title': title,
				'author' : author,
				'date_created': date_created,
				'comment_count': int(comment_count),
				'post_url': post_url,
				'post_content': post_content
			}

