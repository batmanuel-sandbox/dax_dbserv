#!/usr/bin/env python

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
This is a program for running RESTful LSST Database Server (only).
Use it for tests. It is really meant to run as part of the central
Web Service, e.g., through webserv/bin/server.py

@author  Jacek Becla, SLAC
"""

from flask import Flask, request
import json
import logging as log
import sys
from lsst.dax.dbserv import dbREST_v0
from lsst.db.engineFactory import getEngineFromFile

app = Flask(__name__)

# Configure Engine
defaults_file = "~/.lsst/dbAuth-dbServ.ini"
engine = getEngineFromFile(defaults_file)
app.config["default_engine"] = engine


@app.route('/')
def application_root():
    """In standalone mode, this handles requests above the tap service"""
    fmt = request.accept_mimetypes.best_match(['application/json', 'text/html'])
    s = "tap"
    if fmt == "text/html":
        return s
    return json.dumps(s)


app.register_blueprint(dbREST_v0.dbREST, url_prefix='/tap')

if __name__ == '__main__':
    log.basicConfig(
        format='%(asctime)s %(name)s %(levelname)s: %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S',
        level=log.DEBUG)

    try:
        app.run(debug=True)
    except Exception as e:
        print "Problem starting the server.", str(e)
        sys.exit(1)
