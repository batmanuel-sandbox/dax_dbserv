# LSST Data Management System
# Copyright 2015 AURA/LSST.
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
This module implements the TAP and TAP-like protocols for access
to a database.

Supported formats: json and html.

@author  Kenny Lo, SLAC
"""

import json
import logging as log
from http.client import OK, INTERNAL_SERVER_ERROR

from flask import Blueprint, request, current_app, make_response, render_template
from flask import redirect


dbRESTv1 = Blueprint('dbRESTv1', __name__, template_folder='templates')


@dbRESTv1.route('/', methods=['GET'])
def root():
    fmt = request.accept_mimetypes.best_match(['application/json', 'text/html'])
    if fmt == 'text/html':
        return "Hello, LSST TAP Service v1 here. I currently support: " \
               "<a href='query'>/db/v1/sync</a>."
    return "Hello, LSST Database Service v1 here. I currently support: /db/v1/sync."


@dbRESTv1.route('/db/v1/sync', methods=['GET', 'POST'])
def sync_query():
    """Synchronously run a query by redirecting to albuquery.
    :return: A proper response object
    """
    url_for_albuquery = "http://localhost:8080/sync"
    return redirect(url_for_albuquery, data=request.form.get("data"), code=302)
