"""
KBC Environment Handler.
"""
import csv
import json
import os
from _datetime import timedelta
from collections import Counter

import dateparser
import math
import pytz
from datetime import datetime
from dateutil.relativedelta import relativedelta
from keboola import docker
from typing import List

from kbc.result import KBCResult

from .logger import get_logger

LOGGER = get_logger()

DEFAULT_DEL = ','
DEFAULT_ENCLOSURE = '"'


class KBCEnvHandler:
    """
    Class handling standard tasks for KBC component manipulation i.e. config load, validation

    It contains some useful methods helping with boilerplate tasks.
    """
    def __init__(self, mandatory_params, data_path=None):
        """

        Args:
            mandatory_params (array(str)): Array of parameter names that needs to be present in the config.json.
            May be nested, see :func:`KBCEnvHandler.validateConfig()` docs for more details.

            data_path (str): optional path to data folder - if not specified data folder fetched
                             from KBC_DATADIR if present, otherwise '/data' as default
            env variable by default.
        """
        if not data_path and os.environ.get('KBC_DATADIR'):
            data_path = os.environ.get('KBC_DATADIR')
        elif not data_path:
            data_path = '/data'

        self.kbc_config_id = os.environ.get('KBC_CONFIGID')

        self.data_path = data_path
        self.configuration = docker.Config(data_path)
        self.cfg_params = self.configuration.get_parameters()
        self.image_params = self.configuration.config_data["image_parameters"]
        self.tables_out_path = os.path.join(data_path, 'out', 'tables')
        self.tables_in_path = os.path.join(data_path, 'in', 'tables')

        self._mandatory_params = mandatory_params

    # ==============================================================================

    def validate_config(self, mandatory_params=[]):
        """
                Validates config parameters based on provided mandatory parameters.
                All provided parameters must be present in config to pass.
                ex1.:
                par1 = 'par1'
                par2 = 'par2'
                mandatory_params = [par1, par2]
                Validation will fail when one of the above parameters is not found

                Two levels of nesting:
                Parameters can be grouped as arrays par3 = [groupPar1, groupPar2]
                => at least one of the pars has to be present
                ex2.
                par1 = 'par1'
                par2 = 'par2'
                par3 = 'par3'
                groupPar1 = 'groupPar1'
                groupPar2 = 'groupPar2'
                group1 = [groupPar1, groupPar2]
                group3 = [par3, group1]
                mandatory_params = [par1, par2, group1]

                Folowing logical expression is evaluated:
                Par1 AND Par2 AND (groupPar1 OR groupPar2)

                ex3
                par1 = 'par1'
                par2 = 'par2'
                par3 = 'par3'
                groupPar1 = 'groupPar1'
                groupPar2 = 'groupPar2'
                group1 = [groupPar1, groupPar2]
                group3 = [par3, group1]
                mandatory_params = [par1, par2, group3]

                Following logical expression is evaluated:
                par1 AND par2 AND (par3 OR (groupPar1 AND groupPar2))
                """
        return self.validate_parameters(self.cfg_params, mandatory_params, 'config parameters')

    def validate_image_parameters(self, mandatory_params):
        """
                Validates image parameters based on provided mandatory parameters.
                All provided parameters must be present in config to pass.
                ex1.:
                par1 = 'par1'
                par2 = 'par2'
                mandatory_params = [par1, par2]
                Validation will fail when one of the above parameters is not found

                Two levels of nesting:
                Parameters can be grouped as arrays par3 = [groupPar1, groupPar2]
                => at least one of the pars has to be present
                ex2.
                par1 = 'par1'
                par2 = 'par2'
                par3 = 'par3'
                groupPar1 = 'groupPar1'
                groupPar2 = 'groupPar2'
                group1 = [groupPar1, groupPar2]
                group3 = [par3, group1]
                mandatory_params = [par1, par2, group1]

                Folowing logical expression is evaluated:
                Par1 AND Par2 AND (groupPar1 OR groupPar2)

                ex3
                par1 = 'par1'
                par2 = 'par2'
                par3 = 'par3'
                groupPar1 = 'groupPar1'
                groupPar2 = 'groupPar2'
                group1 = [groupPar1, groupPar2]
                group3 = [par3, group1]
                mandatory_params = [par1, par2, group3]

                Following logical expression is evaluated:
                par1 AND par2 AND (par3 OR (groupPar1 AND groupPar2))
                """
        return self.validate_parameters(self.image_params, mandatory_params, 'image/stack parameters')

    def validate_parameters(self, parameters, mandatory_params, _type):
        """
        Validates provided parameters based on provided mandatory parameters.
        All provided parameters must be present in config to pass.
        ex1.:
        par1 = 'par1'
        par2 = 'par2'
        mandatory_params = [par1, par2]
        Validation will fail when one of the above parameters is not found

        Two levels of nesting:
        Parameters can be grouped as arrays par3 = [groupPar1, groupPar2] => at least one of the pars has to be present
        ex2.
        par1 = 'par1'
        par2 = 'par2'
        par3 = 'par3'
        groupPar1 = 'groupPar1'
        groupPar2 = 'groupPar2'
        group1 = [groupPar1, groupPar2]
        group3 = [par3, group1]
        mandatory_params = [par1, par2, group1]

        Folowing logical expression is evaluated:
        Par1 AND Par2 AND (groupPar1 OR groupPar2)

        ex3
        par1 = 'par1'
        par2 = 'par2'
        par3 = 'par3'
        groupPar1 = 'groupPar1'
        groupPar2 = 'groupPar2'
        group1 = [groupPar1, groupPar2]
        group3 = [par3, group1]
        mandatory_params = [par1, par2, group3]

        Following logical expression is evaluated:
        par1 AND par2 AND (par3 OR (groupPar1 AND groupPar2))
        """
        missing_fields = []
        for field in mandatory_params:
            if isinstance(field, list):
                missing_fields.extend(self._validate_par_group(field, parameters))
            elif not parameters.get(field):
                missing_fields.append(field)

        if missing_fields:
            raise ValueError(
                'Missing mandatory {} fields: [{}] '.format(_type, ', '.join(missing_fields)))

    def _validate_par_group(self, par_group, parameters):
        missing_fields = []
        is_present = False
        for par in par_group:
            if isinstance(par, list):
                missing_subset = self._get_par_missing_fields(par, parameters)
                missing_fields.extend(missing_subset)
                if not missing_subset:
                    is_present = True

            elif parameters.get(par):
                is_present = True
            else:
                missing_fields.append(par)
        if not is_present:
            return missing_fields
        else:
            return []

    def _get_par_missing_fields(self, mand_params, parameters):
        missing_fields = []
        for field in mand_params:
            if not parameters.get(field):
                missing_fields.append(field)
        return missing_fields

    def get_storage_token(self):
        try:
            return os.environ["KBC_TOKEN"]
        except Exception:
            LOGGER.error("Storage token is missing in KBC_TOKEN env variable.")
            exit(2)

    def get_authorization(self):
        """
        Returns a dictionary containing the authentication part of the configuration file. If not present,
        an exception is raised.
        The dictionary returned has the following form:
        {
            "id": "main",
            "authorizedFor": "Myself",
            "creator": {
              "id": "1234",
              "description": "me@keboola.com"
            },
            "created": "2016-01-31 00:13:30",
            "#data": "{\"refresh_token\":\"MCWBkfdK9m5YK*$\"}",
            "oauthVersion": "2.0",
            "appKey": "000003423433C184A49",
            "#appSecret": "sdsadasdas-CiN"
        }
        """

        try:
            return self.configuration.config_data["authorization"]["oauth_api"]["credentials"]
        except KeyError:
            LOGGER.error("Authorization is missing in configuration file.")
            exit(2)

    def get_input_table_by_name(self, table_name):
        tables = self.configuration.get_input_tables()
        table = [t for t in tables if t.get('destination') == table_name]
        if not table:
            raise ValueError(
                'Specified input mapping [{}] does not exist'.format(table_name))
        return table[0]

    def get_image_parameters(self):
        return self.configuration.config_data["image_parameters"]

    def get_state_file(self):
        """

        Return dict representation of state file or nothing if not present

        Returns:
            dict:

        """
        LOGGER.info('Loading state file...')
        state_file_path = os.path.join(self.data_path, 'in', 'state.json')
        if not os.path.isfile(state_file_path):
            LOGGER.info('State file not found. First run?')
            return
        try:
            with open(state_file_path, 'r') \
                    as state_file:
                return json.load(state_file)
        except (OSError, IOError):
            raise ValueError(
                "State file state.json unable to read "
            )

    def write_state_file(self, state_dict):
        """
        Stores state file.
        Args:
            state_dict:
        """
        if not isinstance(state_dict, dict):
            raise TypeError('Dictionary expected as a state file datatype!')

        with open(os.path.join(self.configuration.data_dir, 'out', 'state.json'), 'w+') as state_file:
            json.dump(state_dict, state_file)

    def create_sliced_tables(self, folder_name, pkey=None, incremental=False,
                             src_delimiter=DEFAULT_DEL, src_enclosure=DEFAULT_ENCLOSURE, dest_bucket=None):
        """
        Creates prepares sliced tables from all files in DATA_PATH/out/tables/{folder_name} - i.e. removes all headers
        and creates single manifest file based on provided parameters.

        Args:
            folder_name: folder name present in DATA_PATH directory that contains files for slices,
        the same name will be used as table name
            pkey: array of pkeys
            incremental: boolean
            src_delimiter: delimiter of the source file [,]
            src_enclosure: enclosure of the source file ["]
            dest_bucket: name of the destination bucket, eg. in.c-input (optional)


        """
        LOGGER.info('Creating sliced tables for [{}]..'.format(folder_name))

        folder_path = os.path.join(self.tables_out_path, folder_name)

        if not os.path.isdir(folder_path):
            raise ValueError("Specified folder ({}) does not exist in the data folder ({})".format(
                folder_name, self.data_path))

        # get files
        files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isfile(
            os.path.join(folder_path, f))]

        header = self.get_and_remove_headers_in_all(
            files, src_delimiter, src_enclosure)
        if dest_bucket:
            destination = dest_bucket + '.' + folder_name
        else:
            destination = folder_name

        LOGGER.info('Creating manifest file...')
        self.configuration.write_table_manifest(
            file_name=folder_path, destination=destination, primary_key=pkey, incremental=incremental, columns=header)

    def get_and_remove_headers_in_all(self, files, delimiter, enclosure):
        """
        Removes header from all specified files and return it as a list of strings

        Throws error if there is some file with different header.

        """
        first_run = True
        for file in files:
            curr_header = self._get_and_remove_headers(
                file, delimiter, enclosure)
            if first_run:
                header = curr_header
                first_file = file
                first_run = False
            # check whether header matches
            if Counter(header) != Counter(curr_header):
                raise Exception('Header in file {}:[{}] is different than header in file {}:[{}]'.format(
                    first_file, header, file, curr_header))
        return header

    def _get_and_remove_headers(self, file, delimiter, enclosure):
        """
        Removes header from specified file and return it as a list of strings.
        Creates new updated file 'upd_'+origFileName and deletes the original
        """
        head, tail = os.path.split(file)
        with open(file, "r") as input_file:
            with open(os.path.join(head, 'upd_' + tail), 'w+', newline='') as updated:
                reader = csv.DictReader(
                    input_file, delimiter=delimiter, quotechar=enclosure)
                header = reader.fieldnames
                writer = csv.DictWriter(
                    updated, fieldnames=header, delimiter=DEFAULT_DEL, quotechar=DEFAULT_ENCLOSURE)
                for row in reader:
                    # write row
                    writer.writerow(row)
        os.remove(file)
        return header

    def process_results(self, res_files, def_bucket_name, output_bucket):
        for res in res_files:
            dest_bucket = def_bucket_name + str(self.kbc_config_id)
            if output_bucket:
                suffix = '-' + output_bucket
            else:
                suffix = ''

            # build manifest
            self.configuration.write_table_manifest(
                file_name=res['full_path'],
                destination=dest_bucket + suffix + '.' + res['name'],
                primary_key=res['pkey'],
                incremental=True)

    def process_results_sliced(self, res_files):
        res_sliced_folders = {}
        for file in res_files:
            res_sliced_folders.update({file['name']: file['pkey']})

        for folder in res_sliced_folders:
            self.create_sliced_tables(folder, res_sliced_folders[folder], True)

    def create_manifests(self, results: List[KBCResult], headless=False, incremental=True):
        """
        Write manifest files for the results produced by kbc.results.ResultWriter
        :param results: List of result objects
        :param headless: Flag whether results contain sliced headless tables and hence
        the `.column` attribute should be
        used in manifest file.
        :param incremental:
        :return:
        """
        for r in results:
            if not headless:
                self.configuration.write_table_manifest(r.full_path, r.table_def.destination,
                                                        r.table_def.pk,
                                                        None, incremental, r.table_def.metadata,
                                                        r.table_def.column_metadata)
            else:
                self.configuration.write_table_manifest(r.full_path, r.table_def.destination,
                                                        r.table_def.pk,
                                                        r.table_def.columns, incremental,
                                                        r.table_def.metadata,
                                                        r.table_def.column_metadata)

    # ==============================================================================
    # == UTIL functions

    def get_date_period_converted(self, period_from, period_to):
        """
        Returns given period parameters in datetime format, or next step in back-fill mode
        along with generated last state for next iteration.

        :param period_from: str YYYY-MM-DD or relative string supported by date parser e.g. 5 days ago
        :param period_to: str YYYY-MM-DD or relative string supported by date parser e.g. 5 days ago

        :return: start_date: datetime, end_date: datetime
        """

        start_date_form = dateparser.parse(period_from)
        end_date_form = dateparser.parse(period_to)
        day_diff = (end_date_form - start_date_form).days
        if day_diff < 0:
            raise ValueError("start_date cannot exceed end_date.")

        return start_date_form, end_date_form

    def get_backfill_period(self, period_from, period_to, last_state):
        """
        Get backfill period, either specified period in datetime type or period based on a previous run (last_state)
        Continues iterating date periods based on the initial period size defined by from and to parameters.
        ex.:
        Run 1:
        _get_backfill_period("2018-01-01", "2018-01-03", None ) -> datetime("2018-01-01"),datetime("2018-01-03"),state)

        Run 2:
        _get_backfill_period("2018-01-01", "2018-01-03", last_state(from previous) )
                -> datetime("2018-01-03"), datetime("2018-01-05"), state)

        etc...

        :type last_state: dict
        - None or state file produced by backfill mode
        e.g. {"last_period" : {
                                "start_date": "2018-01-01",
                                "end_date": "2018-01-02"
                                }
            }

        :type period_to: str YYYY-MM-DD format or relative string supported by date parser e.g. 5 days ago
        :type period_from: str YYYY-MM-DD format or relative string supported by date parser e.g. 5 days ago
        :rtype: start_date: datetime, end_date: datetime, state_file: dict
        """
        if last_state and last_state.get('last_period'):
            last_start_date = datetime.strptime(
                last_state['last_period']['start_date'], '%Y-%m-%d')
            last_end_date = datetime.strptime(
                last_state['last_period']['end_date'], '%Y-%m-%d')

            diff = last_end_date - last_start_date
            # if period is a single day
            if diff.days == 0:
                diff = timedelta(days=1)

            start_date = last_end_date
            if (last_end_date.date() + diff) >= datetime.now(pytz.utc).date() + timedelta(days=1):
                end_date = datetime.now(pytz.utc)
            else:
                end_date = last_end_date + diff
        else:
            start_date = dateparser.parse(period_from)
            end_date = dateparser.parse(period_to)
        return start_date, end_date

    def get_past_date(self, str_days_ago: str, to_date: datetime = None,
                      tz: pytz.tzinfo.BaseTzInfo = pytz.utc) -> object:
        """
        Returns date in specified timezone relative to today.

        e.g.
        '5 hours ago',
        'yesterday',
        '3 days ago',
        '4 months ago',
        '2 years ago',
        'today'

        :param str_days_ago: (str)
        :param to_date: (datetime)
        :param tz: (pytz.tzinfo.BaseTzInfo)
        :return:
        """
        if to_date:
            TODAY = to_date
        else:
            TODAY = datetime.datetime.now(tz)

        try:
            today_diff = (datetime.datetime.now(tz) - TODAY).days
            past_date = dateparser.parse(str_days_ago)
            past_date.replace(tzinfo=tz)
            date = past_date - relativedelta(days=today_diff)
            return date
        except TypeError:
            raise ValueError(
                "Please enter valid date parameters. Some of the values (%s, %s)are not in supported format",
                str_days_ago)

    def split_dates_to_chunks(self, start_date, end_date, intv, strformat="%m%d%Y"):
        """
        Splits dates in given period into chunks of specified max size.

        Params:
        start_date -- start_period [datetime]
        end_date -- end_period [datetime]
        intv -- max chunk size
        strformat -- dateformat of result periods

        Usage example:
        list(split_dates_to_chunks("2018-01-01", "2018-01-04", 2, "%Y-%m-%d"))

            returns [{start_date: "2018-01-01", "end_date":"2018-01-02"}
                     {start_date: "2018-01-02", "end_date":"2018-01-04"}]
        """
        return list(self._split_dates_to_chunks_gen(start_date, end_date, intv, strformat))

    def _split_dates_to_chunks_gen(self, start_date, end_date, intv, strformat="%m%d%Y"):
        """
        Splits dates in given period into chunks of specified max size.

        Params:
        start_date -- start_period [datetime]
        end_date -- end_period [datetime]
        intv -- max chunk size
        strformat -- dateformat of result periods

        Usage example:
        list(split_dates_to_chunks("2018-01-01", "2018-01-04", 2, "%Y-%m-%d"))

            returns [{start_date: "2018-01-01", "end_date":"2018-01-02"}
                     {start_date: "2018-01-02", "end_date":"2018-01-04"}]
        """

        nr_days = (end_date - start_date).days

        if nr_days <= intv:
            yield {'start_date': start_date.strftime(strformat),
                   'end_date': end_date.strftime(strformat)}
        elif intv == 0:
            diff = timedelta(days=1)
            for i in range(nr_days):
                yield {'start_date': (start_date + diff * i).strftime(strformat),
                       'end_date': (start_date + diff * i).strftime(strformat)}
        else:
            nr_parts = math.ceil(nr_days / intv)
            diff = (end_date - start_date) / nr_parts
            for i in range(nr_parts):
                yield {'start_date': (start_date + diff * i).strftime(strformat),
                       'end_date': (start_date + diff * (i + 1)).strftime(strformat)}
