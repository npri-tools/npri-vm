import os
from flask import Flask, render_template, jsonify
from sqlalchemy import create_engine, text, URL
import pandas
import json
import urllib.parse

# Flask DB Function
def db(sql):
  url_object = URL.create("postgresql",
                        username=os.environ.get("U"),
                        password=urllib.parse.unquote(os.environ.get("P")),  # plain (unescaped) text
                        host=os.environ.get("HO"),
                        database=os.environ.get("D"),
                        port=5432
                        )
  engine = create_engine(url_object, isolation_level = "AUTOCOMMIT")

  data = None
  try:
    with engine.connect() as conn, conn.begin():
      sql = text(sql)
      print("db: ", sql) # Debugging
      data = pandas.read_sql_query(sql, conn)
  except:
    print("Database connection issue") # Convert to error
  engine.dispose()
  
  return data

# Turn the parameters passed through the API into a SQL query
def sqlize(view, params):
  """
  construct a sql query from a view (e.g. Facilities), a filter (e.g near), and value (e.g. [43.5,-80.25])
  """
  preset_views = {"facilities": "npri_exporter_table ", "places": "npri_screen_table ", 
                  "industry": "npri_industry_table ", "company": "npri_companies_table ", 
                  "substance": "npri_tri_table ", "time_company": 'history_company_table ',
                  "time_place": 'history_da_table ', "time_substance": 'history_substance_table '
                  }
  presets_pid = {"facilities": {"AB":1,"BC":2,"MB":3,"NB":4,"NL":5,"NT":6,"NS":7,"NU":8,"ON":9,"PE":10,"QC":11,"SK":12,"YT":13, "":""},
                "places": {"AB":48,"BC":59,"MB":46,"NB":13,"NL":10,"NT":61,"NS":12,"NU":62,"ON":35,"PE":11,"QC":24,"SK":47,"YT":60, "":""},
                 "industry": {"":""}, "substance": {"":""}, "company": {"":""}, "time_company": {"":""}, "time_place": {"":""}, "time_substance": {"":""}
                 }
  preset_indexes = {"facilities": "NpriID", "places": "dauid", "industry": "NAICSPrimary", "company": "CompanyId", "substance": "SubstanceID",
                    "time_company": 'CompanyID',"time_place": 'DA', "time_substance": 'Substance'
                    }
  index = preset_indexes[view] # Preset ID
  sql = 'select * from ' + preset_views[view]
  print("sqlizing: ", view, params)

  #params = params #"ids=1,15,11;substance=lead,mercury;near=43.5,-80.25"
  params = params.split(";")
  keys = {"substances":"", "ids":"", "near":",", "naics":"", "across":"", "bounds":"", 
          "place":"", "companies": "", "pollutants": "", "industries": "", "years": ",",
          "within":""
          }
  passed_keys = []
  for p in params:
    t = p.split("=")
    passed_keys.append(t[0])
    keys[t[0]] = t[1]

  sql = 'select * from ' + preset_views[view] + 'where '
  print("sql", sql)
  sqls = {
      "substances" : 'lower("Substances") like any (array[{}])'.format(','.join('\'%'+s.lower()+'%\'' for s in keys["substances"].split(","))),
      "ids" : '"{}" in ({})'.format(index, ','.join(str(idx) for idx in keys["ids"].split(","))),
      "near" : "ST_INTERSECTS(geom,ST_Buffer(ST_Transform(ST_SetSRID(ST_MakePoint({}, {}), 4326), 3347), 10000))".format(str(keys["near"].split(",")[1]), str(keys["near"].split(",")[0])),
      "naics" : '"NACIS" in ({})'.format(','.join("%"+s+"%" for s in keys["naics"].split(","))),
      "across" : '"ProvinceID" = {}'.format(presets_pid[view][keys["across"]]),
      "place" : '"ForwardSortationArea" like any (array[{}])'.format(','.join('\'%'+fsa+'%\'' for fsa in keys["place"].split(","))),
      "bounds" : 'ST_INTERSECTS(geom,ST_Transform(ST_SetSRID(ST_MakeEnvelope({}), 4326), 3347))'.format(','.join(c for c in keys["bounds"].split(","))),
      "companies" : 'lower("CompanyName") like any (array[{}])'.format(','.join('\'%'+s.lower()+'%\'' for s in keys["companies"].split(","))),
      "pollutants" : 'lower("Substance") like any (array[{}])'.format(','.join('\'%'+s.lower()+'%\'' for s in keys["pollutants"].split(","))),
      "industries" : 'lower("NAICSTitleEn") like any (array[{}])'.format(','.join('\'%'+s.lower()+'%\'' for s in keys["industries"].split(","))),
      "years" : '"ReportYear" >= {} and "ReportYear" <= {}'.format(keys["years"].split(",")[0], keys["years"].split(",")[1]),
      "within": '"dauid" = any (array[{}])'.format(','.join(str(da) for da in keys["within"].split(",")))
  }
  print("sqls", sqls)
  for pk in passed_keys:
    sql += sqls[pk] + " and "
  sql = sql[:-5] # Remove trailing end

  """
  else:
    sql += 'limit 5' # Prevent empty calls like Places() from querying the whole db
  """

  print("sqlized: ", sql)
  return sql

app = Flask(__name__)

# Homepage
@app.route('/')
def home():
  return render_template('index.html')

# API - get data from e.g. a notebook environment based on pre-defined queries
@app.route('/api/<application>/<view>/<params>')
def api(application, view, params):
  sql = sqlize(view, params)
  print("returned sql: ", sql)
  data = db(sql)
  print("returned data: ", data)
  if application == "data":
    return data.to_json() 
  elif application == "report":
    #  # Do stuff with data
    return render_template(
      view + '.html',
      view = view,
      params = params,
      table = [data.to_html(header="true")]
    )

# SQL - get data from e.g. a notebook environment based on SQL
@app.route('/sql/<sql>')
def sql(sql):
  sql = urllib.parse.unquote_plus(sql)
  data = db(sql)
  print("returned data: ", data)
  return data.to_json()

if __name__ == '__main__':
  server_port = os.environ.get('PORT', '8080')
  app.run(debug=False, port=server_port, host='0.0.0.0')
