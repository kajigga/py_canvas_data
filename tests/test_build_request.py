#!/usr/bin/env python
# -*- coding: utf-8 -*-
from unittest import TestCase
from pprint import pprint
import os, time, glob

import subprocess

from canvas_data_utils.canvas_data_auth import CanvasData, required_sections

API_KEY = '82c864690fe61af93e7d53eddee56161cee75cc0'
API_SECRET = '4558d04f0af00c461bf1fbe787f10ba24dd3f5fe'

BASE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'test_dir')
DATA_BASE_DIR = os.path.join(BASE_DIR,'test2')

class TestingCanvasData(TestCase):

  def setUp(self):
    self.cd = CanvasData(
        API_KEY=API_KEY ,
        API_SECRET=API_SECRET, 
        base_folder = BASE_DIR,
        data_folder = DATA_BASE_DIR)

  def tearDown(self):
    self.cd.remove_caches()

  def test_build_request(self):
    path = '/api/schema/latest'
    from datetime import datetime
    _date = datetime(2016,3,24,9,50).strftime('%a, %d %b %y %H:%M:%S GMT')

    signature,_date_returned,headers = self.cd._buildRequest(path, _date)

    good_signature = "VstsIKJkXfib4bVOG+0qeOGb5FevBu9PSCdqTpswJqw="
    good_headers =  {
        'Authorization': 'HMACAuth 82c864690fe61af93e7d53eddee56161cee75cc0:VstsIKJkXfib4bVOG+0qeOGb5FevBu9PSCdqTpswJqw=',
        'Date': 'Thu, 24 Mar 16 09:50:00 GMT'}
    assert signature == good_signature
    print('signature', signature)
    assert _date_returned == _date
    assert headers == good_headers
    requests.get('http://requestb.in/1myx0oj1?{}'.format(path), header=headers)
    assert False
  
  #def test_get_schema2(self):
  #  self.cd.schema
  #  assert False


