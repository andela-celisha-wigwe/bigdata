#!/usr/bin/env python
# -*- coding: utf-8 -*-

import fileinput
import json
import re
import sys
gt_reg = re.compile("(&gt);")
lt_reg = re.compile("(&lt;)")
html_reg = re.compile("(<[^><]*>)")
style_script_reg = re.compile("(<(script|style)[^<>]*>[^<>]*<\/(style|script)>)")

lines = json.loads(sys.stdin.read())

# fileinput.input(openhook=fileinput.hook_encoded("utf-8")):
# content = json.loads(line)
for content in lines:
	content["post_content"] = gt_reg.sub(">", content["post_content"])
	content["post_content"] = lt_reg.sub("<", content["post_content"])
	content["post_content"] = style_script_reg.sub("", content["post_content"])
	# content["post_content"] = html_reg.sub("", content["post_content"])
	while html_reg.search(content["post_content"]):
		content["post_content"] = html_reg.sub('', content["post_content"])
print json.dumps(lines, indent=2)
