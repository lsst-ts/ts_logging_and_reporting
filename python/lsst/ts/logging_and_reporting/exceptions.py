# This file is part of ts_logging_and_reporting.
#
# Developed for Vera C. Rubin Observatory Telescope and Site Systems.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

# error_code values should be no bigger than 8 characters 12345678
import traceback
from warnings import warn

class BaseLogrepException(Exception):
    is_an_error_response = True
    status_code = 400
    error_message = '<NA>'
    saved_tb = None

    def get_subclass_name(self):
        return self.__class__.__name__

    def __init__(self, error_message, error_code=None, status_code=None):
        Exception.__init__(self)
        self.error_message = error_message
        if error_code:
            assert len(error_code) <= 8, f'error_code "{error_code}" too big'
            self.error_code = error_code

        if error_code is not None:
            self.error_code = error_code
        if status_code is not None:
            self.status_code = status_code or self.status_code

        self.saved_tb = traceback.format_exc()

    def __str__(self):
        return (f'[{self.error_code}] {self.error_message}'
                f' {self.saved_tb=}')

    def to_dict(self):
        dd = dict(errorMessage=self.error_message,
                  errorCode=self.error_code,
                  #! trace=self.saved_tb,
                  statusCode=self.status_code)
        return dd


example_error_from_exposurelog = {
    'detail': [
        {'type': 'int_parsing',
         'loc': ['query', 'min_day_obs'],
         'msg': 'Input should be a valid integer, unable to parse string as an integer',
         'input': '2024-08-19'},
        {'type': 'int_parsing',
        'loc': ['query', 'max_day_obs'],
         'msg': 'Input should be a valid integer, unable to parse string as an integer',
         'input': '2024-09-21'}]}


class BadStatus(BaseLogrepException):
    """Non-200 HTTP status from API endpoint. Typically
    this will occur when a URL query string parameter is passed a value with
    a bad format.  It may also be that the Service is broken.
    """
    error_code = 'BADQSTR'
