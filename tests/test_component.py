"""
Created on 12. 11. 2018

@author: esner
"""
import filecmp
import os
import shutil
import tempfile
import unittest

import mock
from freezegun import freeze_time

try:
    from component import Component
except ImportError:
    from src.component import Component


class TestComponent(unittest.TestCase):

    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {'KBC_DATADIR': './non-existing-dir'})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()

    def test_dedupe_binlog_result(self):
        temp_directory = tempfile.TemporaryDirectory().name
        os.makedirs(temp_directory, exist_ok=True)
        temp_file = os.path.join(temp_directory, 'test.csv')
        shutil.copy('./resources/test.csv', os.path.join(temp_directory, 'test.csv'))

        Component.deduplicate_binlog_result(temp_file, ['Start_Date', 'End_Date', 'Campaign_Name'])

        outcome = filecmp.cmp(temp_file, './resources/deduped.csv', shallow=False)
        self.assertTrue(outcome)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
