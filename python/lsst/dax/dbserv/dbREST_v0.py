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

@author  Jacek Becla, SLAC

"""

import json
import logging as log
from http.client import OK, INTERNAL_SERVER_ERROR, CREATED
import traceback

from flask import Blueprint, request, current_app, make_response, \
    render_template, jsonify, url_for
from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, InterfaceError

from lsst.dax.dbserv.compat.fields import MySQLFieldHelper
from lsst.dax.webservcommon import render_response
from .uws.driver import AsyncDriver, AsyncDriverError
from .model import DriverJob

dbREST = Blueprint('dbREST', __name__, template_folder='templates')


@dbREST.errorhandler(Exception)
def handle_unhandled_exceptions(error):
    log.error("Error handling request:\n {}".format(error))
    log.error(traceback.format_exc())
    err = {
        "exception": error.__class__.__name__,
        "message": error.args[0]
    }

    if len(error.args) > 1:
        err["more"] = [str(arg) for arg in error.args[1:]]
    response = jsonify(err)
    response.status_code = INTERNAL_SERVER_ERROR
    return response


@dbREST.route('/', methods=['GET'])
def root():
    fmt = request.accept_mimetypes.best_match(['application/json', 'text/html'])
    if fmt == 'text/html':
        return "LSST TAP Service v0 here. I currently support: " \
               "<a href='query'>/sync</a>."
    return "LSST Database Service v0 here. I currently support: /sync."


@dbREST.route('/sync', methods=['POST'])
def sync_query():
    """Synchronously run a query.
    :return: A proper response object
    """

    query = request.args.get("query", request.form.get("query", None))
    if query:
        log.debug(query)
        try:
            engine = _get_engine()
            results = []
            helpers = []
            rows = engine.execute(text(query))
            curs = rows.cursor

            for row in rows:
                # If this is the first row, build column definitions
                # (use raw values to help)
                if not helpers:
                    for desc, flags, val in zip(curs.description,
                                                curs.description_flags, row):
                        helpers.append(MySQLFieldHelper(desc, flags, val))

                # Not streaming...
                results.append([helper.check_value(val) for helper, val in zip(helpers, row)])

            status_code = OK
            elements = []
            for helper in helpers:
                field = dict(name=helper.name, datatype=helper.datatype)
                if helper.xtype:
                    field["xtype"] = helper.xtype
                elements.append(field)

            response = _result(dict(metadata=dict(elements=elements), data=results))
        except SQLAlchemyError as e:
            log.debug("Encountered an error processing request: '%s'" % e.message)
            response = _error(type(e).__name__, e.message)
            status_code = INTERNAL_SERVER_ERROR
        return _response(response, None, status_code)
    else:
        return "Listing queries is not implemented."


@dbREST.route('/async', methods=['POST'])
def async_query():
    """Asynchronously run a query.
    :return: A proper response object
    """

    query = request.args.get("query", request.form.get("query", None))
    log.debug(query)
    db_url = _get_db_url_from_query(query)
    async_driver = _get_async_driver(db_url)
    user_context = _get_user_info(request)
    driver_job_id = async_driver.submit(query, db_url, user_context)

    session = Session()

    driver_job = DriverJob(job_id=driver_job_id,
                           driver_name=async_driver.__name__)
    session.add(driver_job)
    session.flush()

    url = url_for(".async_job", job_id=driver_job_id,
                  _external=True)
    response = jsonify({"result": {"jobId": driver_job_id,
                                   "url": url}})
    response.headers["Location"] = url
    response.status_code = CREATED
    return response


@dbREST.route('/async/<string:job_id>/', methods=['GET'])
def async_job(job_id):
    """Asynchronously run a query.
    :return: A proper response object
    """

    session = Session()
    driver_job = session.query(DriverJob).filter(
        DriverJob.job_id == job_id).first()
    driver = _get_async_driver(driver=driver_job.driver)
    job = driver.job(driver_job.job_id)

    url = url_for(".async_query_result", job_id=driver_job_id,
                  _external=True)
    response = jsonify({"result": {"jobId": driver_job_id,
                                   "url": url}})
    response.headers["Location"] = url
    response.status_code = CREATED
    return response


def _get_db_url_from_query(query):
    return _get_engine().url


def _get_user_info(_request):
    return {}


def _get_async_driver(db_url):
    return AsyncDriver()


@event.listens_for(Engine, "handle_error")
def handle_qserv_exception(context):
    conn = context.connection.connection
    if hasattr(conn, "error") and context.original_exception.args[0] == -1:
        # Handle Qserv Errors where we return error codes above those
        # identified by the MySQLdb driver_name.
        # The MySQL driver_name, by default, returns a "whack" error code
        # if this is the case with error == -1.
        from _mysql_exceptions import InterfaceError as MysqlIError
        old_exc = context.sqlalchemy_exception
        orig = MysqlIError(conn.errno(), conn.error())
        return InterfaceError(old_exc.statement, old_exc.params,
                              orig, old_exc.connection_invalidated)
    pass


def _get_engine():
    # Look for a dbserv-specific config URL, otherwise use default engine.
    db_engine = current_app.config.get("dax.dbserv.db.engine", None)
    if not db_engine:
        db_url = current_app.config.get("dax.dbserv.db.url", None)
        if db_url:
            pool_size = current_app.config.get("dax.dbserv.db.pool_size", 10)
            db_engine = create_engine(db_url, pool_size=pool_size)
        else:
            db_engine = current_app.config["default_engine"]
        current_app.config["dax.dbserv.db.engine"] = db_engine
    return db_engine


def _error(exception, message):
    return dict(error=exception, message=message)


def _result(table):
    return dict(result=dict(table=table))


votable_mappings = {
    "text": "unicodeChar",
    "binary": "unsignedByte"
}


def _response(response, _request, status_code):
    fmt = request.accept_mimetypes.best_match(['application/json', 'text/html',
                                               'application/x-votable+xml'])
    if fmt == 'text/html':
        response = render_response(response=response, status_code=status_code)
    elif fmt == 'application/x-votable+xml':
        response = render_template('votable.xml.j2',
                                   result=response["result"],
                                   mappings=votable_mappings)
    else:
        response = json.dumps(response)
    return make_response(response, status_code)
