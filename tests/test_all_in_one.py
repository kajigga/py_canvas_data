#!/usr/bin/env python
# -*- coding: utf-8 -*-
from unittest import TestCase
from pprint import pprint
import os, time, glob

import subprocess

from canvas_data_utils.canvas_data_auth import CanvasData, required_sections

API_KEY    = '85d7e4e8691d1ea8e2a69745c5741a27c87791e6'
API_SECRET = '97ad5aa5f33e95404e793f6657a14d7718bcf724'

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

  def test_init_class(self):
    output = self.cd.table_creation_statement('mysql')
    self.assertTrue('CREATE TABLE' in output)

    postgres_output = self.cd.table_creation_statement('postgresql')
    self.assertTrue('CREATE TABLE' in postgres_output)

    sqlite_output = self.cd.table_creation_statement('sqlite')
    self.assertTrue('CREATE TABLE' in sqlite_output)

  def test_create_tables(self):
    db_filename = os.path.join(BASE_DIR,'testfile3.db')
    self.cd.create_tables('sqlite:///{}'.format(db_filename))

    # file should exist
    self.assertTrue(os.path.exists(db_filename))

    os.remove(db_filename)

  def test_tables_md(self):
    self.cd.tables_md()
    base_keys = self.cd.base.classes.keys()
    base_keys.sort()
    tlist = self.cd.table_list()
    tlist.sort()
    self.assertEqual(base_keys, tlist)
    self.assertEqual(len(tlist), 56)
    self.assertEqual(len(base_keys), 56)


  def test_print_schema(self):
    output = self.cd.print_schema(print_output=False)
    self.assertTrue(output)

class TestingImports(TestCase):

  def setUp(self):
    self.cd = CanvasData(
        API_KEY=API_KEY,
        API_SECRET=API_SECRET, 
        base_folder = BASE_DIR,
        data_folder = DATA_BASE_DIR)

    db_str = 'sqlite:///'
    self.cd.set_connection(db_str)
    self.cd.create_tables()

  def test_import_data(self):
    base_keys = self.cd.base.classes.keys()
    tlist = self.cd.table_list()

    self.assertEqual(sorted(base_keys), sorted(tlist))


  def test_import_account(self):
    self.cd.import_data('account')
    self.assertTrue('account' in self.cd.table_list())
        
  def test_import_assignment_dim(self):
    self.cd.import_data('assignment_dim')
    self.assertTrue('assignment_dim' in self.cd.table_list())

  def test_import_assignment_fact(self):
    self.cd.import_data('assignment_fact')
    self.assertTrue('assignment_fact' in self.cd.table_list())

  def test_import_assignment_group_dim(self):
    self.cd.import_data('assignment_group_dim')
    self.assertTrue('assignment_group_dim' in self.cd.table_list())

  def test_import_assignment_group_fact(self):
    self.cd.import_data('assignment_group_fact')
    self.assertTrue('assignment_group_fact' in self.cd.table_list())

  def test_import_assignment_group_rule_dim(self):
    self.cd.import_data('assignment_group_rule_dim')
    self.assertTrue('assignment_group_rule_dim' in self.cd.table_list())

  def test_import_assignment_rule_dim(self):
    self.cd.import_data('assignment_rule_dim')
    self.assertTrue('assignment_rule_dim' in self.cd.table_list())

  def test_import_communication_channel_dim(self):
    self.cd.import_data('communication_channel_dim')
    self.assertTrue('communication_channel_dim' in self.cd.table_list())

  def test_import_communication_channel_fact(self):
    self.cd.import_data('communication_channel_fact')
    self.assertTrue('communication_channel_fact' in self.cd.table_list())

  def test_import_conversation(self):
    self.cd.import_data('conversation')
    self.assertTrue('conversation' in self.cd.table_list())

  def test_import_conversation_message(self):
    self.cd.import_data('conversation_message')
    self.assertTrue('conversation_message' in self.cd.table_list())

  def test_import_conversation_message_participant(self):
    self.cd.import_data('conversation_message_participant')
    self.assertTrue('conversation_message_participant' in self.cd.table_list())

  def test_import_course(self):
    self.cd.import_data('course')
    self.assertTrue('course' in self.cd.table_list())

  def test_import_course_section(self):
    self.cd.import_data('course_section')
    self.assertTrue('course_section' in self.cd.table_list())

  def test_import_course_ui_navigation_item_dim(self):
    self.cd.import_data('course_ui_navigation_item_dim')
    self.assertTrue('course_ui_navigation_item_dim' in self.cd.table_list())

  def test_import_course_ui_navigation_item_fact(self):
    self.cd.import_data('course_ui_navigation_item_fact')
    self.assertTrue('course_ui_navigation_item_fact' in self.cd.table_list())

  def test_import_discussion_entry_dim(self):
    self.cd.import_data('discussion_entry_dim')
    self.assertTrue('discussion_entry_dim' in self.cd.table_list())

  def test_import_discussion_entry_fact(self):
    self.cd.import_data('discussion_entry_fact')
    self.assertTrue('discussion_entry_fact' in self.cd.table_list())

  def test_import_discussion_topic_dim(self):
    self.cd.import_data('discussion_topic_dim')
    self.assertTrue('discussion_topic_dim' in self.cd.table_list())

  def test_import_discussion_topic_fact(self):
    self.cd.import_data('discussion_topic_fact')
    self.assertTrue('discussion_topic_fact' in self.cd.table_list())

  def test_import_enrollment_dim(self):
    self.cd.import_data('enrollment_dim')
    self.assertTrue('enrollment_dim' in self.cd.table_list())

  def test_import_enrollment_fact(self):
    self.cd.import_data('enrollment_fact')
    self.assertTrue('enrollment_fact' in self.cd.table_list())

  def test_import_enrollment_rollup(self):
    self.cd.import_data('enrollment_rollup')
    self.assertTrue('enrollment_rollup' in self.cd.table_list())

  def test_import_enrollment_term(self):
    self.cd.import_data('enrollment_term')
    self.assertTrue('enrollment_term' in self.cd.table_list())

  def test_import_external_tool_activation_dim(self):
    self.cd.import_data('external_tool_activation_dim')
    self.assertTrue('external_tool_activation_dim' in self.cd.table_list())

  def test_import_external_tool_activation_fact(self):
    self.cd.import_data('external_tool_activation_fact')
    self.assertTrue('external_tool_activation_fact' in self.cd.table_list())

  def test_import_group_dim(self):
    self.cd.import_data('group_dim')
    self.assertTrue('group_dim' in self.cd.table_list())

  def test_import_group_fact(self):
    self.cd.import_data('group_fact')
    self.assertTrue('group_fact' in self.cd.table_list())

  def test_import_group_membership_fact(self):
    self.cd.import_data('group_membership_fact')
    self.assertTrue('group_membership_fact' in self.cd.table_list())

  def test_import_pseudonym_dim(self):
    self.cd.import_data('pseudonym_dim')
    self.assertTrue('pseudonym_dim' in self.cd.table_list())

  def test_import_pseudonym_fact(self):
    self.cd.import_data('pseudonym_fact')
    self.assertTrue('pseudonym_fact' in self.cd.table_list())

  def test_import_quiz_dim(self):
    self.cd.import_data('quiz_dim')
    self.assertTrue('quiz_dim' in self.cd.table_list())

  def test_import_quiz_fact(self):
    self.cd.import_data('quiz_fact')
    self.assertTrue('quiz_fact' in self.cd.table_list())

  def test_import_quiz_question_answer_dim(self):
    self.cd.import_data('quiz_question_answer_dim')
    self.assertTrue('quiz_question_answer_dim' in self.cd.table_list())

  def test_import_quiz_question_answer_fact(self):
    self.cd.import_data('quiz_question_answer_fact')
    self.assertTrue('quiz_question_answer_fact' in self.cd.table_list())

  def test_import_quiz_question_dim(self):
    self.cd.import_data('quiz_question_dim')
    self.assertTrue('quiz_question_dim' in self.cd.table_list())

  def test_import_quiz_question_fact(self):
    self.cd.import_data('quiz_question_fact')
    self.assertTrue('quiz_question_fact' in self.cd.table_list())

  def test_import_quiz_question_group_dim(self):
    self.cd.import_data('quiz_question_group_dim')
    self.assertTrue('quiz_question_group_dim' in self.cd.table_list())

  def test_import_quiz_question_group_fact(self):
    self.cd.import_data('quiz_question_group_fact')
    self.assertTrue('quiz_question_group_fact' in self.cd.table_list())

  def test_import_quiz_submission_dim(self):
    self.cd.import_data('quiz_submission_dim')
    self.assertTrue('quiz_submission_dim' in self.cd.table_list())

  def test_import_quiz_submission_fact(self):
    self.cd.import_data('quiz_submission_fact')
    self.assertTrue('quiz_submission_fact' in self.cd.table_list())

  def test_import_quiz_submission_historical_dim(self):
    self.cd.import_data('quiz_submission_historical_dim')
    self.assertTrue('quiz_submission_historical_dim' in self.cd.table_list())

  def test_import_quiz_submission_historical_fact(self):
    self.cd.import_data('quiz_submission_historical_fact')
    self.assertTrue('quiz_submission_historical_fact' in self.cd.table_list())

  def test_import_requests(self):
    self.cd.import_data('requests')
    self.assertTrue('requests' in self.cd.table_list())

  def test_import_role_dim(self):
    self.cd.import_data('role_dim')
    self.assertTrue('role_dim' in self.cd.table_list())

  def test_import_submission_comment_dim(self):
    self.cd.import_data('submission_comment_dim')
    self.assertTrue('submission_comment_dim' in self.cd.table_list())

  def test_import_submission_comment_fact(self):
    self.cd.import_data('submission_comment_fact')
    self.assertTrue('submission_comment_fact' in self.cd.table_list())

  def test_import_submission_comment_participant_dim(self):
    self.cd.import_data('submission_comment_participant_dim')
    self.assertTrue('submission_comment_participant_dim' in self.cd.table_list())

  def test_import_submission_comment_participant_fact(self):
    self.cd.import_data('submission_comment_participant_fact')
    self.assertTrue('submission_comment_participant_fact' in self.cd.table_list())

  def test_import_submission_dim(self):
    self.cd.import_data('submission_dim')
    self.assertTrue('submission_dim' in self.cd.table_list())

  def test_import_submission_fact(self):
    self.cd.import_data('submission_fact')
    self.assertTrue('submission_fact' in self.cd.table_list())

  def test_import_user(self):
    self.cd.import_data('user')
    self.assertTrue('user' in self.cd.table_list())

  def test_import_wiki_dim(self):
    self.cd.import_data('wiki_dim')
    self.assertTrue('wiki_dim' in self.cd.table_list())

  def test_import_wiki_fact(self):
    self.cd.import_data('wiki_fact')
    self.assertTrue('wiki_fact' in self.cd.table_list())

  def test_import_wiki_page_dim(self):
    self.cd.import_data('wiki_page_dim')
    self.assertTrue('wiki_page_dim' in self.cd.table_list())

  def test_importwiki_page_fact(self):
    self.cd.import_data('wiki_page_fact')
    self.assertTrue('wiki_page_fact' in self.cd.table_list())



class TestReadConfig(TestCase):
  def setUp(self):
    config_filename = os.path.join(BASE_DIR, 'config.ini') 
    self.cd = CanvasData(config_filename=config_filename)
        

  def test_read_valid_config_file(self):
    config_filename = os.path.join(BASE_DIR, 'config.ini') 
    self.assertTrue(os.path.exists(config_filename))
    conf, valid = self.cd.config_valid(config_filename)
    self.assertTrue(valid)


  def test_read_config_options(self):
    config_filename = os.path.join(BASE_DIR, 'config.ini') 
    self.cd.parse_config_file(config_filename)

    for rs in required_sections:
      self.assertTrue(self.cd._config.has_section(rs))
    for rs in required_sections:
      for option in required_sections[rs]:
        self.assertTrue(self.cd._config.has_option(rs,option))
        self.assertIsNotNone(self.cd._config.get(rs, option))

  def test_read_invalid_config_file(self):
    config_filename = os.path.join(BASE_DIR, 'config_invalid.ini') 
    self.assertTrue(os.path.exists(config_filename))
    config,valid = self.cd.config_valid(config_filename)
    self.assertFalse(valid)

class TestCLI(TestCase):

  def setUp(self):
    # Remove all *.csv files from the test2 folder
    files = glob.glob(os.path.join(DATA_BASE_DIR,'*.csv'))
    for f in files:
      os.unlink(f)

  def test_cli_with_arguments_fails(self):
    has_error = False
    try:
      script_output = subprocess.check_output(['canvasdata'], **{})  
    except subprocess.CalledProcessError:
      has_error = True
    self.assertTrue(has_error)

  def test_cli_displays_help(self):
    script_output = subprocess.check_output(['canvasdata','-h'], **{})  
    self.assertTrue('usage: canvasdata [-h] [--config CONFIG]' in script_output)

  def test_cli_convert_to_csv(self):
    config_filename = os.path.join(BASE_DIR, 'config.ini') 
    cmd = ['canvasdata','--config', config_filename, 'convert_to_csv']
    self.assertEqual(glob.glob(os.path.join(DATA_BASE_DIR,'*.csv')),[])

    script_output = subprocess.check_output(cmd)  
    self.assertNotEqual(glob.glob(os.path.join(DATA_BASE_DIR,'*.csv')),[])
