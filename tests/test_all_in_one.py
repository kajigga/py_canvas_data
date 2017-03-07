#!/usr/bin/env python
# -*- coding: utf-8 -*-
from unittest import TestCase
from pprint import pprint
import os
import time
import glob

import subprocess

from canvas_data_utils.canvas_data_auth import CanvasData, required_sections

API_KEY = os.environ['CANVASDATA_API_KEY']
API_SECRET = os.environ['CANVASDATA_API_SECRET']

BASE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'test_dir')
# DATA_BASE_DIR = os.path.join(BASE_DIR)
DATA_BASE_DIR = BASE_DIR


class TestingCanvasData(TestCase):

    def setUp(self):
        self.cd = CanvasData(
            API_KEY=API_KEY,
            API_SECRET=API_SECRET,
            base_folder=BASE_DIR,
            data_folder=DATA_BASE_DIR)

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
        db_filename = os.path.join(BASE_DIR, 'testfile3.db')
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
        self.assertEqual(len(tlist), 65)
        self.assertEqual(len(base_keys), 65)

    def test_print_schema(self):
        output = self.cd.print_schema(print_output=False)
        self.assertTrue(output)


class TestingImports(TestCase):

    def setUp(self):
        self.cd = CanvasData(
            API_KEY=API_KEY,
            API_SECRET=API_SECRET,
            base_folder=BASE_DIR,
            data_folder=DATA_BASE_DIR)

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

    def test_import_external_tool_activation_fact(self):
        self.cd.import_data('external_tool_activation_fact')
        self.assertTrue('external_tool_activation_fact' in self.cd.table_list())

    def test_import_role_dim(self):
        self.cd.import_data('role_dim')
        self.assertTrue('role_dim' in self.cd.table_list())


class TestReadConfig(TestCase):
    def setUp(self):
        config_filename = os.path.join(BASE_DIR, 'config.ini')
        print('config_filename', config_filename)
        self.cd = CanvasData(config_filename=config_filename)

    def test_read_valid_config_file(self):
        config_filename = os.path.join(BASE_DIR, 'config.ini')
        self.assertTrue(os.path.exists(config_filename))
        conf, valid, errors = self.cd.config_valid(config_filename)
        self.assertTrue(valid)
        self.assertEqual([], errors)

    def test_read_config_options(self):
        config_filename = os.path.join(BASE_DIR, 'config.ini')
        self.cd.parse_config_file(config_filename)

        for rs in required_sections:
            self.assertTrue(self.cd._config.has_section(rs))
        for rs in required_sections:
            for option in required_sections[rs]:
                self.assertTrue(self.cd._config.has_option(rs, option))
                self.assertIsNotNone(self.cd._config.get(rs, option))

    def test_read_invalid_config_file(self):
        config_filename = os.path.join(BASE_DIR, 'config_invalid.ini')
        self.assertTrue(os.path.exists(config_filename))
        config, valid, errors = self.cd.config_valid(config_filename)
        self.assertFalse(valid)
        self.assertNotEquals([], errors)

def files_with_extension(folder, *extensions):
    files = []

    for ext in extensions:
        files.extend(glob.glob(os.path.join(folder, '*.'+ext)))

    return files


class TestCLI(TestCase):

    def setUp(self):
        # Remove all *.csv files from the test2 folder
        self.config_filename = os.path.join(BASE_DIR, 'config.ini')
        self.cd = CanvasData(config_filename=self.config_filename)
        self.db_dir = self.cd._config.get('config', 'base_folder')

    def tearDown(self):

        for f in files_with_extension(self.cd.data_folder, 'csv', 'gz'):
            os.unlink(f)

    def test_cli_with_arguments_fails(self):
        has_error = False
        try:
            script_output = subprocess.check_output(['canvasdata'], **{})
        except subprocess.CalledProcessError:
            has_error = True
        self.assertTrue(has_error)

    def test_cli_displays_help(self):
        script_output = subprocess.check_output(['canvasdata', '-h'], **{})
        self.assertTrue(b'usage: canvasdata [-h] [--config CONFIG]' in script_output)

    def test_cli_convert_to_csv(self):
        cmd1 = ['canvasdata', '--config', self.config_filename, '-t', 'user', 'download']
        cmd2 = ['canvasdata', '--config', self.config_filename, '-t', 'user', 'convert_to_csv']

        self.assertEqual(files_with_extension(self.cd.data_folder, 'csv'), [])

        script_output = subprocess.check_output(cmd1)
        script_output = subprocess.check_output(cmd2)
        self.assertNotEqual(files_with_extension(self.cd.data_folder, 'csv'), [])
