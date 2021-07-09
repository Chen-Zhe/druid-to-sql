# ============ helper functions ==============
def druid_filter_to_sql(filt):
  filters = list()
  operator = ""
  
  if filt['type'] == "or" or filt['type'] == "and":
    operator = f" {filt['type']} "
    for sub_filter in filt["fields"]:
      filters.append(druid_filter_to_sql(sub_filter))
      
  elif filt['type'] == "selector":
    value = filt['value']
    return f"\"{filt['dimension']}\" = '{value}'"
  elif filt['type'] == "search":
    # assume it's just contains
    return f"contains(\"{filt['dimension']}\", '{filt['query']['value']}')"
  elif filt['type'] == "not":
    field = filt["field"]
    cond = druid_filter_to_sql(field)
    if cond is not None:
      return f"not {cond}"
    else:
      return None
  elif filt['type'] == "bound":
    operator = " and "
    if "lower" in filt:
      filters.append(f"\"{filt['dimension']}\" {'>' if 'lowerStrict' in filt and filt['lowerStrict'] else '>='} {filt['lower']}")
    if "upper" in filt:
      filters.append(f"\"{filt['dimension']}\" {'<' if 'upperStrict' in filt and filt['upperStrict'] else '<='} {filt['upper']}")
  elif filt['type'] == "in":
    values = filt['values']    
    value_str = ", ".join([f"'{v}'" for v in values])
    return f"\"{filt['dimension']}\" in ({value_str})"
  elif filt['type'] == "like":
    pattern = filt['pattern']
    return f"\"{filt['dimension']}\" like '{pattern}'"
  else:
    raise Exception(f"Unknown filter type '{filt['type']}'")
    
  valid_filters = [f for f in filters if f is not None]
  if len(valid_filters) > 0:
    return "(" + operator.join(valid_filters) + ")"
  else:
    return None


def get_field_name(name):
  if type(name) is str:
    return name.replace('"', '\\"')
  else:
    return name["name"]


def druid_aggregations_to_sql(agg):
  if type(agg) is list:
    converted = [druid_aggregations_to_sql(a) for a in agg]
    converted = [c for c in converted if c is not None]
    if len(converted) > 0:
      return ", ".join(converted)
    else:
      return None
  
  if agg["type"] == "cardinality":
    quoted_fields = [f'"{field}"' for field in agg['fields']]
    return f"COUNT (DISTINCT {','.join(quoted_fields)}) AS \"{get_field_name(agg['name'])}\""
  elif agg["type"].endswith("Sum"):
    return f"SUM (\"{agg['fieldName']}\") AS \"{get_field_name(agg['name'])}\""
  elif agg["type"] == "count":
    return f"COUNT (*) AS \"{get_field_name(agg['name'])}\""
  elif agg["type"].endswith("Last") or agg["type"].endswith("Max"):
    return f"MAX (\"{agg['fieldName']}\") AS \"{get_field_name(agg['name'])}\""
  elif agg["type"].endswith("First") or agg["type"].endswith("Min"):
    return f"MIN (\"{agg['fieldName']}\") AS \"{get_field_name(agg['name'])}\""
  elif agg["type"] == "filtered":
    if agg["aggregator"]["type"] == "count":
      ops = "COUNT ("
      field = "1"
    elif agg["aggregator"]["type"] == "cardinality":
      ops = "COUNT (DISTINCT"
      field = '"' + '","'.join(agg["aggregator"]["fields"]) + '"'
      
    elif agg["aggregator"]["type"].endswith("Last") or agg["aggregator"]["type"].endswith("Max"):
      ops = "MAX"
      field = agg["aggregator"]['fieldName']
    elif agg["aggregator"]["type"].endswith("First") or agg["aggregator"]["type"].endswith("Min"):
      ops = "MIN"
      field = agg["aggregator"]['fieldName']
  
    elif agg["aggregator"]["type"].endswith("Sum"):
      ops = "SUM ("
      try:
        field = '"' + agg["aggregator"]["fieldName"] + '"'
      except:
        raise Exception("error")
    else:
      raise Exception(f"Unknown aggregator type '{agg['aggregator']['type']}'")

    filter_condition = druid_filter_to_sql(agg['filter'])
    
    if filter_condition is not None:
      return f"{ops} CASE WHEN {filter_condition} THEN {field} END) AS \"{get_field_name(agg['aggregator']['name'])}\" "
    else:
      return None
  else:
    raise Exception(f"Unknown aggregation '{agg['type']}'")
# ==========================


# main converter function
def convert_to_sql(query):
  filter_conds = list()
  
  query_type = query["queryType"]
  
  def gen_cols(raw_columns, alias=True):
    columns = list()
    for col in raw_columns:
      alias_str = ""
      
      if type(col) is str:
        if col == "__time":
          col_name = "time"
        else:
          col_name = col
      else:
        if col["type"].endswith("Filtered"):
          col = col["delegate"]
          
        col_name = col['dimension']
        if alias:
          alias_str = f" AS \"{get_field_name(col['outputName'])}\""
      
      columns.append(f"\"{col_name}\"{alias_str}")
    return ", ".join(columns)
  
  table = query["dataSource"]
  
  if type(table) is dict:
    table = f"({convert_to_sql(table['query'])})"
  else:
    table = table.replace("-", "_") # Snowflake doesn't support '-'
    
    # only append interval for inner query
    intervals = query["intervals"]

    if type(intervals) is str:
      intervals = [intervals]

    for interval in intervals:
      start, end = interval.split("/")
      filter_conds.append(f"\"time\" >= '{start}' and \"time\" <= '{end}'")
  
  if "filter" in query and query["filter"]:
    filter_conds.append(druid_filter_to_sql(query["filter"]))
  
  filter_str = " AND ".join(filter_conds)
  
  valid_filters = [f for f in filter_conds if f is not None]
  if len(valid_filters) > 0:
    filter_str = "WHERE " + " AND ".join(filter_conds)
  else:
    filter_str = ""
  
  group_by_str = ""
  order_by_str = ""
  
  if query_type == "scan":
    column_str = gen_cols(query["columns"])
  elif query_type == "timeseries":
    column_str = f"{druid_aggregations_to_sql(query['aggregations'])}"
  elif query_type == "topN":
    dimension = query["dimension"]
    metric = query["metric"]
    threshold = query["threshold"]
    column_str = f"TOP {threshold} {gen_cols([dimension], False)}"
    group_by_str = f"GROUP BY {gen_cols([dimension], False)}"
    order_by_str = f"ORDER BY {gen_cols([metric], False)} DESC"
    if "aggregations" in query:
      column_str += f", {druid_aggregations_to_sql(query['aggregations'])}"
  elif query_type == "groupBy":
    columns = list()
    if "dimensions" in query:
      dims = query["dimensions"]
      columns.append(gen_cols(dims))
      group_by_str = f"GROUP BY {gen_cols(dims, False)}"
      
    if "aggregations" in query:
      columns.append(f"{druid_aggregations_to_sql(query['aggregations'])}")
    column_str = ",".join(columns)
#     if "postAggregations" in query:
#       raise Exception("aggregations in groupby")
  else:
    raise Exception(f"Unknown query type '{query_type}'")
  
  return f"SELECT {column_str} FROM {table} {filter_str} {group_by_str} {order_by_str}"


# test
if __name__ == '__main__':
  import json

  query = """
  {
    "queryType": "timeseries",
    "intervals": ["2020-06-24T00:00:00.000Z/2020-06-30T00:00:00.000Z"],
    "aggregations":[
      {"type": "filtered",
        "filter": {"type": "selector", "dimension": "Dim1", "value": "Value1"},
        "aggregator": {
          "type": "cardinality", "name": "Dim2", "fields": ["Dim3"], "round": true
        }
      },
      {"type": "cardinality", "name": "Dim7", "fields": ["Dim3"], "round": true}
    ],
    "dataSource": "SampleDataSource",
    "filter": { "type": "and",
      "fields": [
        {"type": "selector", "dimension": "Dim3", "value": "Value3"},
        {"type": "in", "dimension": "Dim4", "values": ["Value4", "Value5"]}
      ]
    },
    "granularity": "all"
  }
  """

  print(convert_to_sql(json.loads(query)))