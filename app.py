import sqlite3

from flask import Flask
from flask import g
from flask import jsonify
from flask import request

import db_driver

app = Flask(__name__)
sqlite_file = 'db.sqlite3'


def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(sqlite_file)
        db.row_factory = sqlite3.Row
    return db


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


@app.before_first_request
def activate_job():
    with app.app_context():
        db_driver.refreshdb(get_db().cursor())
        get_db().commit()


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/search/stop')
def searchstop():
    query = request.args['find']
    if str.isdigit(query):
        result = get_db().execute('SELECT * FROM stops WHERE id=? LIMIT 1', (query,))
        result = result.fetchone()
        return jsonify(dict(result))
    else:
        result = get_db().execute('SELECT * from stops WHERE name like ? order by id', ('%' + query + '%',))
        result = result.fetchall()
        query = []
        for each in result:
            query.append(dict(each))
        return jsonify(query)


@app.route('/stop')
def searchtripsbyid():
    query = request.args['id']
    result = get_db().execute('SELECT * from times where stop_id=?', (query,))
    result = result.fetchall()
    query = []
    for each in result:
        query.append(dict(each))
    return jsonify(query)


if __name__ == '__main__':
    app.run(host='0.0.0.0')
