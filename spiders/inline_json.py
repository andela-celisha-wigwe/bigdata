#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import fileinput
import sys

lines = json.loads(sys.stdin.read())

for content in lines:
	print json.dumps(content)
