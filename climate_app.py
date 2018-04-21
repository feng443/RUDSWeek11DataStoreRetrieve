'''

<Chan Feng> 2018-04-21 Rutgers Data Science Week 11 Climate App

'''

from flask import Flask, jsonify
import pandas as pd
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, func
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

_SQLITE_FILE = 'hawaii.sqlite'
engine = create_engine(f'sqlite:///{_SQLITE_FILE}')
Base = automap_base()
Base.prepare(engine, reflect=True)
Measurement = Base.classes.measurement
Station = Base.classes.station

END_DATE = Session(engine).query(func.max(Measurement.date)).all()[0][0]
START_DATE = END_DATE - relativedelta(months=12)

app = Flask(__name__)

@app.route('/')
def root():

    msg = '''
    <h1>Welcome to the climate API!</h1>
    
    Available Routes:
    
    <ul>
        <li><a href="/api/v1.0/precipitation">/api/v1.0/precipitation</a>
        <li><a href="/api/v1.0/stations">/api/v1.0/stations</a>
        <li><a href="/api/v1.0/tobs">/api/v1.0/tobs</a>
        <li><a href="/api/v1.0/2017-01-01">/api/v1.0/YYYY-MM-DD</a>
        <li><a href="/api/v1.0/2016-01-01/2017-01-01">/api/v1.0/YYYY-MM-DD/YYYY-MM-DD</a>
    </ul>
    '''
    print(msg)
    return msg

@app.route('/api/v1.0/precipitation')
def precipitation():
    return jsonify(
        {
            d.strftime('%Y-%m-%d'): v
            for d, v in
            Session(engine).query(
                Measurement.date, func.avg(Measurement.prcp)
            ).filter(
                Measurement.date.between(START_DATE, END_DATE)
            ).group_by(
                Measurement.date
            ).all()
        }
    )

@app.route('/api/v1.0/stations')
def stations():
    return jsonify(pd.read_sql_table('station', engine).to_dict('record'))

@app.route('/api/v1.0/tobs')
def tobs():
    return jsonify(
        {
            d.strftime('%Y-%m-%d'): v
            for d, v in
            Session(engine).query(
                Measurement.date, func.avg(Measurement.tobs)
            ).filter(
                Measurement.date.between(START_DATE, END_DATE)
            ).group_by(
                Measurement.date
            ).all()
        }
    )

@app.route('/api/v1.0/<start>')
def min_avg_max_temp_start(start):
    start = start or '1700-01-01'
    tobs = Measurement.tobs
    return jsonify(list(
        Session(engine).query(
            func.min(tobs), func.avg(tobs), func.max(tobs)
        ).filter(
            Measurement.date >= start
        ).all()[0]
    ))


@app.route('/api/v1.0/<start>/<end>')
def min_avg_max_temp_start_end(start, end):
    start = start or '1700-01-01'
    end = end or '9999-12-31'
    print(start, end)
    tobs = Measurement.tobs
    return jsonify(list(
        Session(engine).query(
            func.min(tobs), func.avg(tobs), func.max(tobs)
        ).filter(
            Measurement.date.between(start, end)
        ).all()[0]
    ))

if __name__ == '__main__':
    app.run(debug=True)

