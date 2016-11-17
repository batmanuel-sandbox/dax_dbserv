# LSST Data Management System
# Copyright 2016 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.

"""
This module is intended to help us introspect DBAPI cursor
metadata and rows to determine field types for a given result set.
"""

from base64 import b64encode
from decimal import Decimal

import MySQLdb
from MySQLdb.constants.FLAG import BINARY


class MySQLFieldHelper:
    def __init__(self, description, flags, value):
        """
        Helper class to define a column, get it's type for conversion, and convert types if needed.
        This class works on a best-effort basis. It's not guaranteed to be 100% correct.
        @param flags: Flags from MySQLdb.constants.FLAGS
        @param value: An example value type to help with inferring how to convert
        """
        self.datatype = None
        self.xtype = None
        self.converter = None
        self.name = description[0]

        type_code = description[1]
        scale = description[5]

        if type_code in MySQLdb.NUMBER:
            # Use python types first, fallback on float otherwise (e.g. NoneType)
            if isinstance(value, int):
                self.datatype = "int"
            else:
                # If there's a scale, use double, otherwise assume long
                self.datatype = "double" if scale else "long"
                """
                TODO: Default to the actual value so long as x is not of type Decimal.
                If x is a decimal, convert to string and back to float
                This is inefficient, but we need to support fixed point numbers to
                fix it properly.
                """
                self.converter = lambda x: x if not isinstance(x, Decimal) else float(str(x))

        # Check datetime and date
        if type_code in MySQLdb.DATETIME:
            self.datatype = "text"
            self.xtype = "timestamp"
            self.converter = lambda x: x.isoformat()
        if type_code in MySQLdb.DATE:
            self.datatype = "text"
            self.xtype = "date"
            self.converter = lambda x: x.isoformat()

        # Check if this is binary data and potentially unsafe for JSON
        if not self.datatype and flags & BINARY and type_code not in MySQLdb.TIME:
            # This needs to be checked BEFORE the next type check
            self.datatype = "binary"
            self.converter = b64encode
        elif isinstance(value, str):
            self.datatype = "text"

        if not self.datatype:
            # Just return a string and make sure to convert it to string if we don't know about
            # this type. This may include datetime.time
            self.datatype = "text"
            self.converter = str

    def check_value(self, value):
        """
        Check the value returned from the DBAPI.
        @param value:
        @return: The value itself, or a stringified version if it needs to be stringified.
        """
        if self.converter and value:
            return self.converter(value)
        return value
