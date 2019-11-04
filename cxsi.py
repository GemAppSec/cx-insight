#!/usr/bin/env python

"""cxsi.py: CxSAST scan usage insight.

    Requires Python 3.6+
"""

__author__ = 'Randy Geyer'
__copyright__ = 'Copyright 2019, Checkmarx'
__credits__ = ['Robert Nilsson', 'Chris Merritt']
__version__ = '1.0.0'
__maintainer__ = 'Randy Geyer'
__email__ = 'randy@checkmarx.com'
__banner__ = 'cxsi.py: CxSAST scan usage insight.'
__status__ = 'Development'

import os
from os import path
from collections import OrderedDict
from datetime import datetime, timedelta, date
import dateutil
import json
import logging
import pprint
from timeit import default_timer as timer
from typing import (Dict, List, Any)
import sys

import click  # command line parser
import colorama
import pandas as pd  # scan_data_file analysis
import xlsxwriter as excel

# check min python runtime
MIN_PYTHON = (3, 6)
if sys.version_info < MIN_PYTHON:
    sys.exit("Python %s.%s or later is required.\n" % MIN_PYTHON)


# Helper classes
class Args:
    """Script argument scan_data_file holder"""

    def __init__(self, customer: str, scan_data_file: str, excel_file: str, force: bool, debug: bool):
        self.customer = customer
        self.scan_data_file = scan_data_file
        self.excel_file = excel_file
        self.force = force
        self.debug = debug

    def print(self):
        click.echo("{} v{}".format(__banner__, __version__))
        click.echo('Args: ')
        click.echo('{}customer={}'.format('\t', self.customer))
        click.echo('{}scan_data_file={}'.format('\t', self.scan_data_file))
        click.echo('{}excel_file={}'.format('\t', self.excel_file))
        click.echo('{}force={}'.format('\t', self.force))
        click.echo('{}debug={}'.format('\t', self.debug))

    def __str__(self):
        return "Args: customer={}; scan_data_file={}; excel_file={}; force={}; debug={}".format(
            self.customer, self.scan_data_file, self.excel_file, self.force, self.debug)


# Globals
_args: Args
_log: logging.Logger
_scans_wb: excel.Workbook
_wb_formats: Dict[str, excel.format.Format] = {}
_workbooks_props: Dict[str, OrderedDict]
_worksheets: Dict[str, excel.worksheet.Worksheet] = {}

# Constants
SCANS_TABLE_NAME = 'AllScans'


# Print iterations progress
def print_progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█', print_end="\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end=print_end)
    # Print New Line on Complete
    if iteration == total:
        print()


def exit_script(code: int = 0):
    """Cleanly exits script"""
    try:
        if _scans_wb is not None:
            _log.debug('Closing workbook: {}'.format(_args.excel_file))
            _scans_wb.close()
    except NameError:
        pass
    _log.debug("Exiting script with exit code {}".format(code))
    sys.exit(code)


def print_query(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return

    select = 'Id,ProjectName,OwningTeamId,TeamName,ProductVersion,EngineServerId,Origin,PresetName,ScanRequestedOn,' \
             'QueuedOn,EngineStartedOn,EngineFinishedOn,ScanCompletedOn,ScanDuration,FileCount,LOC,FailedLOC,High,' \
             'Medium,Low,Info,IsIncremental,IsLocked,IsPublic'
    filter_ = 'ScanRequestedOn%20gt%202019-05-01T00:00:00.000Z%20and%20ScanRequestedOn%20lt%202019-08-01T00:00:00.000Z'
    languages = 'ScannedLanguages($select=LanguageName)'
    query = 'http://<cxhost>/cxwebinterface/odata/v1/Scans?$select={}&$expand={}&$filter={}'.format(
        select, languages, filter_)
    click.echo("{} v{}".format(__banner__, __version__))
    click.echo(' ')
    click.echo('OData query to generate Scan data:')
    click.echo(' ')
    click.secho(query, fg='green')
    ctx.exit()


def init_logging():
    """Initializes logging.  Specifies a console logger and debug file"""

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s|%(levelname)-8s|%(message)s',
                        # datefmt='',
                        filename='cxsi.log',
                        filemode='w')

    # console handler
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter('%(message)s'))
    # add console handler to the root logger
    logging.getLogger('').addHandler(console)

    global _log
    _log = logging.getLogger('cxsi')
    _log.info('{} v{}'.format(__banner__, __version__))
    _log.info('{}\n'.format(_args))


def init_wb_formats():
    _log.debug("Initializing excel formats...")

    formats: Dict[str, Dict] = {
        'general': {},
        # 'header': {'bold': True, 'text_wrap': False, 'top': 6},
        'header': {'bold': True, 'text_wrap': False},
        'summary header': {'bold': True, 'align': 'center'},

        # Modifiers
        'bold': {'bold': True},
        'center': {'align': 'center'},
        'right': {'align': 'right'},

        # Types
        'date': {'num_format': 'mm/dd/yyyy'},
        'datetime': {'num_format': 'yyyy-mm-dd hh:mm:ss'},
        'date bold': {'bold': True, 'num_format': 'mm/dd/yyyy'},
        'duration': {'num_format': '[h]:mm:ss'},
        'duration bold': {'bold': True, 'num_format': '[h]:mm:ss'},
        'percent': {'num_format': '0%'},
        'decimal 2': {'num_format': '0.00'},
        'integer': {'num_format': 0x01},
        'long': {'num_format': 0x03},

        # Colors
        # white with light blue background, default table style for header row in xlsxwriter
        'header_color': {'bold': 1, 'bg_color': '#4F81BD', 'font_color': '#FFFFFF',
                         'border_color': '#EEECE1', 'bottom': 1},
        # Light red fill with dark red text
        'bad': {'bg_color': '#FFC7CE', 'font_color': '#9C0006'},
        # Light yellow fill with dark yellow text
        'neutral': {'bg_color': '#FFEB9C', 'font_color': '#9C6500'},
        # Green fill with dark green text
        'good': {'bg_color': '#C6EFCE', 'font_color': '#006100'},
        # Black and White
        'inverse': {'bg_color': '#000000', 'font_color': '#FFFFFF'},

        # merge
        'header_merge': {'bold': 1, 'border': 1, 'align': 'center', 'valign': 'vcenter',
                         'fg_color': '#4F81BD', 'font_color': '#FFFFFF', 'border_color': '#EEECE1'}
    }
    return formats


def init_lang_columns():
    """Initializes the language columns"""
    DEFAULT_HIDE = 0

    # Language column starts at 33
    languages = OrderedDict([
        ('Apex', {'col': 33, 'hidden': DEFAULT_HIDE}),
        ('ASP', {'col': 34, 'hidden': DEFAULT_HIDE}),
        ('Cobol', {'col': 35, 'hidden': DEFAULT_HIDE}),
        ('CPP', {'col': 36, 'hidden': DEFAULT_HIDE}),
        ('CSharp', {'col': 37, 'hidden': DEFAULT_HIDE}),
        ('Groovy', {'col': 38, 'hidden': DEFAULT_HIDE}),
        ('Go', {'col': 39, 'hidden': DEFAULT_HIDE}),
        ('Java', {'col': 40, 'hidden': DEFAULT_HIDE}),
        ('JavaScript', {'col': 41, 'hidden': DEFAULT_HIDE}),
        ('Kotlin', {'col': 42, 'hidden': DEFAULT_HIDE}),
        ('Objc', {'col': 43, 'hidden': DEFAULT_HIDE}),
        ('PHP', {'col': 44, 'hidden': DEFAULT_HIDE}),
        ('Perl', {'col': 45, 'hidden': DEFAULT_HIDE}),
        ('Python', {'col': 46, 'hidden': DEFAULT_HIDE}),
        ('Ruby', {'col': 47, 'hidden': DEFAULT_HIDE}),
        ('Scala', {'col': 48, 'hidden': DEFAULT_HIDE}),
        ('Typescript', {'col': 49, 'hidden': DEFAULT_HIDE}),
        ('VbNet', {'col': 50, 'hidden': DEFAULT_HIDE}),
        ('VB6', {'col': 51, 'hidden': DEFAULT_HIDE}),
        # don't track these
        ('Common', {'col': -1, 'hidden': DEFAULT_HIDE}),
        ('PLSQL', {'col': -1, 'hidden': DEFAULT_HIDE}),
        ('VbScript', {'col': -1, 'hidden': DEFAULT_HIDE}),
        ('Unknown', {'col': -1, 'hidden': DEFAULT_HIDE})
    ])
    return languages


def init(args):
    global _args
    _args = args

    init_logging()
    init_workbook(args.excel_file, args.force)


def load_json(file: str) -> OrderedDict:
    """Loads scan data from the supplied json file into a dict.

    If file is not found, exits script.

    :param file: the file to load
    :return: dict containing the json scan_data_file
    """

    file_path: str = path.abspath(file)
    _log.info('Loading scan data from json file: {}'.format(file))

    if path.exists(file) and path.getsize(file):
        _log.debug("Scan json data file found: {}".format(file_path))
    else:
        _log.warning("Scan json data file not found or is empty: {}...exiting".format(file_path))
        exit_script(2)

    with open(file) as json_file:
        try:
            scan_data = json.load(json_file)
            # scan_data = json.load(json_file, object_pairs_hook=OrderedDict)
            _log.info("Loaded json, scan count: {}".format(len(scan_data['value'])))
            if _args.debug:
                _log.debug('First 10 scans...:\n{}'.format(pprint.pformat(scan_data['value'][:10], sort_dicts=False)))
        except json.JSONDecodeError as err:
            _log.critical("Failed to load json: {}".format(err.msg))
            _log.exception(err)
            exit_script(3)

    return scan_data


def init_scans_ws_options(lang_columns):
    """options for scans data table"""
    DEFAULT_COL_WIDTH = 10
    DEFAULT_DATE_WIDTH = 18
    DEFAULT_DURATION_WIDTH = 14
    DEFAULT_RATE_WIDTH = 12
    DEFAULT_RESULT_WIDTH = 8
    DEFAULT_LANG_WIDTH = 8

    options = {
        'name': SCANS_TABLE_NAME,
        'columns': [
            {'header': 'ScanId', 'format': _wb_formats['integer'], 'width': DEFAULT_COL_WIDTH},
            {'header': 'ProjectName', 'format': _wb_formats['general'], 'width': 30},
            {'header': 'ProjectId', 'format': _wb_formats['integer'], 'width': DEFAULT_COL_WIDTH},
            {'header': 'TeamId', 'format': _wb_formats['general'], 'width': 36},
            {'header': 'Team', 'format': _wb_formats['general'], 'width': 15},
            {'header': 'EngineId', 'format': _wb_formats['integer'], 'width': DEFAULT_COL_WIDTH},
            {'header': 'Origin', 'format': _wb_formats['general'], 'width': DEFAULT_COL_WIDTH},
            {'header': 'Preset', 'format': _wb_formats['general'], 'width': 28},
            {'header': 'Incr', 'format': _wb_formats['integer'], 'width': 8},
            {'header': 'LOC', 'format': _wb_formats['integer'], 'width': DEFAULT_COL_WIDTH},
            {'header': 'FailedLOC', 'format': _wb_formats['integer'], 'width': 11},
            {'header': 'FileCount', 'format': _wb_formats['integer'], 'width': DEFAULT_COL_WIDTH},
            {'header': 'ScanRequestedOn', 'format': _wb_formats['datetime'], 'width': DEFAULT_DATE_WIDTH},
            {'header': 'QueuedOn', 'format': _wb_formats['datetime'], 'width': DEFAULT_DATE_WIDTH},
            {'header': 'EngineStartedOn', 'format': _wb_formats['datetime'], 'width': DEFAULT_DATE_WIDTH},
            {'header': 'EngineFinishedOn', 'format': _wb_formats['datetime'], 'width': DEFAULT_DATE_WIDTH},
            {'header': 'ScanCompletedOn', 'format': _wb_formats['datetime'], 'width': DEFAULT_DATE_WIDTH},
            {'header': 'ScanDuration', 'format': _wb_formats['duration'], 'width': DEFAULT_DURATION_WIDTH,
             'formula': '=IF([@ScanCompletedOn]>0,[@ScanCompletedOn]-[@ScanRequestedOn],0)'
             },
            {'header': 'SourceTime', 'format': _wb_formats['duration'], 'width': DEFAULT_DURATION_WIDTH,
             'formula': '=[@QueuedOn]-[@ScanRequestedOn]'
             },
            {'header': 'QueuedTime', 'format': _wb_formats['duration'], 'width': DEFAULT_DURATION_WIDTH,
             'formula': '=IF([@EngineStartedOn]>0,[@EngineStartedOn]-[@QueuedOn],0)'
             },
            {'header': 'EngineTime', 'format': _wb_formats['duration'], 'width': DEFAULT_DURATION_WIDTH,
             'formula': '=IF([@EngineFinishedOn]>0,[@EngineFinishedOn]-[@QueuedOn],0)'
             },
            {'header': 'ScanHours', 'format': _wb_formats['decimal 2'], 'width': DEFAULT_COL_WIDTH,
             'formula': '=[@ScanDuration]*24'
             },
            {'header': 'Weekday', 'format': _wb_formats['integer'], 'width': DEFAULT_COL_WIDTH,
             'formula': '=WEEKDAY([@ScanRequestedOn])'
             },
            {'header': 'FullSpeed', 'format': _wb_formats['integer'], 'width': DEFAULT_RATE_WIDTH,
             'formula': '=IF(AND([@ScanDuration]>0,[@Incr]=0),[@LOC]/([@ScanDuration]*24),0)'
             },
            {'header': 'IncrSpeed', 'format': _wb_formats['integer'], 'width': DEFAULT_RATE_WIDTH,
             'formula': '=IF(AND([@ScanDuration]>0,[@Incr]=1),[@LOC]/([@ScanDuration]*24),0)'
             },
            {'header': 'Results', 'format': _wb_formats['integer'], 'width': DEFAULT_RESULT_WIDTH,
             'formula': '=SUM([@High],[@Med],[@Low],[@Info])'
             },
            {'header': 'High', 'format': _wb_formats['integer'], 'width': DEFAULT_RESULT_WIDTH},
            {'header': 'Med', 'format': _wb_formats['integer'], 'width': DEFAULT_RESULT_WIDTH},
            {'header': 'Low', 'format': _wb_formats['integer'], 'width': DEFAULT_RESULT_WIDTH},
            {'header': 'Info', 'format': _wb_formats['integer'], 'width': DEFAULT_RESULT_WIDTH},
            {'header': 'Version', 'format': _wb_formats['general'], 'width': 13},
            {'header': 'Locked', 'format': _wb_formats['integer'], 'width': 8},
            {'header': 'Public', 'format': _wb_formats['integer'], 'width': 8}
        ]
    }
    for lang in lang_columns:
        lang_col = lang_columns[lang]
        if lang_col['col'] > -1:
            lang_header = {'header': lang, 'format': _wb_formats['integer'], 'width': DEFAULT_LANG_WIDTH}
            options['columns'].append(lang_header)

    return options


def convert_datetime(date_str: str) -> datetime:
    return dateutil.parser.parse(date_str)


def convert_json_scan(scan: OrderedDict, lang_columns):
    scan_row = OrderedDict([
        ('ScanId', {'value': scan['Id'], 'col': 0}),
        ('ProjectName', {'value': scan['ProjectName'], 'col': 1}),
        ('ProjectId', {'value': None, 'col': 2}),
        ('TeamId', {'value': scan['OwningTeamId'], 'col': 3}),
        ('Team', {'value': scan['TeamName'], 'col': 4}),
        ('EngineId', {'value': scan['EngineServerId'], 'col': 5}),
        ('Origin', {'value': scan['Origin'], 'col': 6}),
        ('Preset', {'value': scan['PresetName'], 'col': 7}),
        ('Incr', {'value': 1 if scan['IsIncremental'] else 0, 'col': 8}),
        ('LOC', {'value': scan['LOC'], 'col': 9}),
        ('FailedLOC', {'value': scan['FailedLOC'], 'col': 10}),
        ('FileCount', {'value': scan['FileCount'], 'col': 11}),
        ('ScanRequestedOn', {'value': convert_datetime(scan['ScanRequestedOn']), 'col': 12}),
        ('QueuedOn', {'value': convert_datetime(scan['QueuedOn']), 'col': 13}),
        ('EngineStartedOn', {'value': convert_datetime(scan['EngineStartedOn']), 'col': 14}),
        ('EngineFinishedOn', {'value': convert_datetime(scan['EngineFinishedOn']), 'col': 15}),
        ('ScanCompletedOn', {'value': convert_datetime(scan['ScanCompletedOn']), 'col': 16}),
        ('High', {'value': scan['High'], 'col': 26}),
        ('Med', {'value': scan['Medium'], 'col': 27}),
        ('Low', {'value': scan['Low'], 'col': 28}),
        ('Info', {'value': scan['Info'], 'col': 29}),
        ('Version', {'value': scan['ProductVersion'], 'col': 30}),
        ('Locked', {'value': 1 if scan['IsLocked'] else 0, 'col': 31}),
        ('Public', {'value': 1 if scan['IsPublic'] else 0, 'col': 32})
    ])
    # add language columns
    for lang in scan['ScannedLanguages']:
        lang_name = lang['LanguageName']
        lang_col = lang_columns[lang_name]
        scan_row[lang_name] = {'value': 1, 'col': lang_col['col']}

    return scan_row


def create_scans_wb(excel_file, force):
    excel_path: str = path.abspath(excel_file)
    if path.exists(excel_file):
        if force:
            _log.warning('Excel file exists, overwriting: {}'.format(excel_path))
            try:
                os.remove(excel_file)
            except OSError as err:
                _log.critical('Unable to overwrite excel file: {}'.format(err.strerror))
                exit_script(4)
        else:
            _log.error('Excel file exists: {}\nUse -force flag to overwrite'.format(excel_path))
            exit_script(1)
    else:
        _log.info('Creating Excel file: {}'.format(excel_file))

    wb_options = {'remove_timezone': True, 'default_date_format': 'yyyy-mm-dd hh:mm:ss'}
    scans_wb = excel.Workbook(excel_file, wb_options)
    scans_wb.set_properties({
        'title': '{} CxSAST Usage'.format(_args.customer),
        'subject': 'Scan usage workbook',
        'author': __maintainer__,
        'company': 'Checkmarx',
        'comments': 'Created with Python and XlsxWriter'
    })
    return scans_wb


def init_workbook(excel_file: str, force: bool):
    _log.debug("Initializing excel workbook...")

    global _scans_wb
    _scans_wb = create_scans_wb(excel_file, force)

    # Scan workbook worksheets
    scan_wb_props: OrderedDict = OrderedDict([
        ('Summary', {'color': 'green', 'hidden': False}),
        ('Scans', {'color': 'yellow', 'hidden': False})
        # TODO: add additional pivot table sheets
    ])

    # create worksheets
    global _worksheets
    for ws_key in scan_wb_props:
        ws_def = scan_wb_props[ws_key]
        ws = _scans_wb.add_worksheet(ws_key)
        ws.set_tab_color(ws_def['color'])
        _worksheets[ws_key] = ws

    # Excel formats
    formats: Dict[str, Dict] = init_wb_formats()
    global _wb_formats

    # create wb formats
    for format_ in formats:
        _wb_formats[format_] = _scans_wb.add_format(formats[format_])


def write_scans_ws(scans: List[OrderedDict]):
    """Populates the Scans worksheet"""

    TABLE_TOP_ROWS = 2

    num_scans = len(scans)
    _log.info('Writing scans into worksheet: count={}'.format(num_scans))

    start = timer()

    lang_columns = init_lang_columns()
    options = init_scans_ws_options(lang_columns)
    table_range = 'A{}:AX{}'.format(TABLE_TOP_ROWS, TABLE_TOP_ROWS + num_scans)

    ws = _worksheets['Scans']

    # format header rows
    ws.set_row(0, 14.4, _wb_formats['header_color'])
    merge_format = _wb_formats['header_merge']
    ws.merge_range('M1:Q1', 'Date Timestamps', merge_format)
    ws.merge_range('R1:V1', 'Durations', merge_format)
    ws.merge_range('W1:Y1', 'Rates per hour', merge_format)
    ws.merge_range('Z1:AD1', 'Result counts', merge_format)
    ws.merge_range('AH1:AX1', 'Languages', merge_format)
    ws.freeze_panes(2, 2)

    # set column widths
    col = 0
    for column in options['columns']:
        col_options = {}
        if column['header'] == 'ProjectId':
            col_options = {'hidden': 1}
        ws.set_column(col, col, column['width'], None, col_options)
        col += 1

    # set language columns
    DEFAULT_LANG_COL_WIDTH = 8
    for lang in lang_columns:
        lang_col = lang_columns[lang]
        col = lang_col['col']
        if col > -1:
            col_options = {'hidden': lang_col['hidden']}
            ws.set_column(col, col, DEFAULT_LANG_COL_WIDTH, None, col_options)

    scan_table = ws.add_table(table_range, options)

    # populate data rows
    i = 0
    for scan_item in scans:
        print_progress_bar(i + 1, num_scans)
        scan_row = convert_json_scan(scan_item, lang_columns)
        for scan_col in scan_row:
            cell = scan_row[scan_col]
            ws.write(i + TABLE_TOP_ROWS, cell['col'], cell['value'])
        i += 1

    end = timer()
    _log.info('Done; elapsedTime={:0.0f}ms'.format((end - start) * 1000))


def write_summary_ws(scans: List[OrderedDict]):
    _log.info('Writing summary data...')

    ws = _worksheets['Summary']

    TABLE = SCANS_TABLE_NAME
    bold = _wb_formats['bold']
    header = _wb_formats['summary header']
    integer = _wb_formats['integer']
    date = _wb_formats['datetime']
    duration = _wb_formats['duration']
    decimal = _wb_formats['decimal 2']
    percent = _wb_formats['percent']

    # set column widths
    ws.set_column(0, 0, 5)
    ws.set_column(1, 1, 20)
    ws.set_column(2, 2, 18, integer)
    ws.set_column(3, 3, 10, integer)

    ws.write('A1', '{} Scan Summary Info'.format(_args.customer), bold)

    # scan summary section
    row = 1
    col = 1
    write_headers(ws, row, col, ['Scans', 'Stats'], header)
    write_summary_info(ws, row + 1, col, 'Start Date', '=MIN({}[ScanRequestedOn])'.format(TABLE), date)
    write_summary_info(ws, row + 2, col, 'End Date', '=MAX({}[ScanRequestedOn])'.format(TABLE), date)
    write_summary_info(ws, row + 3, col, 'Days',
                       '=MAX({0}[ScanRequestedOn])-MIN({0}[ScanRequestedOn])'.format(TABLE))
    write_summary_info(ws, row + 4, col, 'Weeks',
                       '=ROUNDUP((MAX({0}[ScanRequestedOn])-MIN({0}[ScanRequestedOn]))/7,0)'.format(TABLE))
    write_summary_info(ws, row + 5, col, 'Scans', '=COUNT({}[ScanId])'.format(TABLE))
    write_summary_info(ws, row + 6, col, 'Completed Scans', '=COUNT({}[ScanCompletedOn])'.format(TABLE))
    write_summary_info(ws, row + 7, col, 'Scans Inflight',
                       '=COUNT({0}[ScanId])-COUNT({0}[ScanCompletedOn])'.format(TABLE))
    write_summary_info(ws, row + 8, col, 'Full Scans',
                       '=COUNTIF({}[Incr],"=0")'.format(TABLE))
    write_summary_info(ws, row + 9, col, 'Incr Scans',
                       '=COUNTIF({}[Incr],"=1")'.format(TABLE))
    ws.write(row+9, col+2, '=COUNTIF({0}[Incr],"=1")/COUNT({0}[ScanId])'.format(TABLE), percent)
    write_summary_info(ws, row + 10, col, 'Avg Full Scan Rate',
                       '=AVERAGEIF({}[FullSpeed],"<>0")'.format(TABLE))
    ws.write(row+10, col+2, 'LOC / Hr')
    write_summary_info(ws, row + 11, col, 'Avg Incr Scan Rate',
                       '=AVERAGEIFS({0}[IncrSpeed],{0}[Incr],"=1 ")'.format(TABLE))
    ws.write(row+11, col+2, 'LOC / Hr')
    write_summary_info(ws, row + 12, col, 'Max Scan Rate',
                       '=MAX({}[FullSpeed])'.format(TABLE))
    ws.write(row+12, col+2, 'LOC / Hr')
    write_summary_info(ws, row + 13, col, 'Avg Scans Per Day',
                       '=COUNT({0}[ScanId])/(MAX({0}[ScanRequestedOn])-MIN({0}[ScanRequestedOn]))'.format(TABLE))
    write_summary_info(ws, row + 14, col, '   Sun',
                       '=COUNTIF({0}[Weekday],"=1")/((MAX({0}[ScanRequestedOn])-MIN({0}[ScanRequestedOn]))/7)'.format(
                           TABLE))
    write_summary_info(ws, row + 15, col, '   Mon',
                       '=COUNTIF({0}[Weekday],"=2")/((MAX({0}[ScanRequestedOn])-MIN({0}[ScanRequestedOn]))/7)'.format(
                           TABLE))
    write_summary_info(ws, row + 16, col, '   Tue',
                       '=COUNTIF({0}[Weekday],"=3")/((MAX({0}[ScanRequestedOn])-MIN({0}[ScanRequestedOn]))/7)'.format(
                           TABLE))
    write_summary_info(ws, row + 17, col, '   Wed',
                       '=COUNTIF({0}[Weekday],"=4")/((MAX({0}[ScanRequestedOn])-MIN({0}[ScanRequestedOn]))/7)'.format(
                           TABLE))
    write_summary_info(ws, row + 18, col, '   Thu',
                       '=COUNTIF({0}[Weekday],"=5")/((MAX({0}[ScanRequestedOn])-MIN({0}[ScanRequestedOn]))/7)'.format(
                           TABLE))
    write_summary_info(ws, row + 19, col, '   Fri',
                       '=COUNTIF({0}[Weekday],"=6")/((MAX({0}[ScanRequestedOn])-MIN({0}[ScanRequestedOn]))/7)'.format(
                           TABLE))
    write_summary_info(ws, row + 20, col, '   Sat',
                       '=COUNTIF({0}[Weekday],"=7")/((MAX({0}[ScanRequestedOn])-MIN({0}[ScanRequestedOn]))/7)'.format(
                           TABLE))


def write_headers(ws, row, col, data: List[str], cell_format={}):
    cur_col = col
    for item in data:
        ws.write(row, cur_col, item, cell_format)
        cur_col += 1


def write_summary_info(ws, row, col, title, data, cell_format={}):
    ws.write(row, col, title)
    ws.write(row, col + 1, data, cell_format)


CONTEXT_SETTINGS = dict(help_option_names=['-?', '-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('-c', '--customer', type=str, required=True,
              help='Customer name')
@click.option('-json', '--scan_data_file', type=click.File('r'), default='scans.json', show_default=True,
              help='Scan data filename to analyze.')
@click.option('-o', '--excel', '--excel_file', type=click.File('w'), default='scans.xlsx', show_default=True,
              help='Output Excel filename.')
@click.option('-f', '--force', is_flag=True, default=False, show_default=True,
              help='Overwrite output file, if it exists.')
@click.option('-d', '--debug', is_flag=True, default=False, show_default=True,
              help='Enable debug logging')
@click.option('-q', '--query', is_flag=True, callback=print_query, expose_value=False,
              help='Prints the odata query used to generate the scan data json and exits.')
@click.version_option(__version__, message='{} v{}'.format(__banner__, __version__))
def cxsi(customer, scan_data_file, excel, force, debug):
    """cxsi.py: Provides CxSAST scan usage insight.

    This script parses and analyzes the supplied CxSAST scan data
    generating an Excel workbook containing the following worksheets:

    \b
    * Summary:  CxSAST summary usage analytics
    * Scans:  data table containing all the scan data for sorting
              and filtering
    TODO: add additional chart/pivot worksheets

    To produce the scan json data file, run the following CxSAST OData
    query:

    http://<host>/cxwebinterface/odata/v1/Scans?$select=Id,ProjectName
    ,OwningTeamId,TeamName,ProductVersion,EngineServerId,Origin,
    PresetName,ScanRequestedOn,QueuedOn,EngineStartedOn,
    EngineFinishedOn,ScanCompletedOn,ScanDuration,FileCount,LOC,
    FailedLOC,High,Medium,Low,Info,IsIncremental,IsLocked,IsPublic
    &$expand=ScannedLanguages($select=LanguageName)
    &$filter=ScanRequestedOn%20gt%202019-05-01T00:00:00.000Z and
    ScanRequestedOn lt 2019-08-01T00:00:00.000Z
    """

    try:
        init(Args(customer, scan_data_file.name, excel.name, force, debug))
        scan_data = load_json(_args.scan_data_file)
        write_scans_ws(scan_data['value'])
        write_summary_ws(scan_data['value'])
    except Exception as err:
        _log.critical('Unexpected exception:')
        _log.exception(err)
    else:
        exit_script()


if __name__ == '__main__':
    cxsi()
