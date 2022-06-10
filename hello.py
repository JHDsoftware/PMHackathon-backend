import json

import pandas as pd
import pm4py
from flask import Flask
from flask import request
from flask import send_file
from flask_cors import CORS, cross_origin
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.visualization.dfg import visualizer as dfg_visualization
from pycelonis import get_celonis
from pycelonis.celonis_api.pql.pql import PQL, PQLFilter, PQLColumn


def strfdelta(tdelta):
    fmt = "{days} days {hours}:{minutes}:{seconds}"
    d = {"days": tdelta.days}
    d["hours"], rem = divmod(tdelta.seconds, 3600)
    d["minutes"], d["seconds"] = divmod(rem, 60)
    return fmt.format(**d)


def timeDiffer(pd1, pd2):
    diff = pd.to_datetime(pd1) - pd.to_datetime(pd2)
    return diff.mean().total_seconds(), diff.std().total_seconds()


celonis = get_celonis(
    url="academic-ang-li3-rwth-aachen-de.eu-2.celonis.cloud",
    api_token="ZGY0OTVlM2UtODUxYy00NWJiLThjNjItMzhkNjVlNDRiMTI1OlpZS1R6dGhwc2FRSmlkampqSURRQk43SE9CclBVcThvUDdibkpZZXV5S0lp"
)
datamodel = celonis.datamodels.find('15ab0e66-43a7-45cb-a688-a2a20c85f19c')
table_name = '"mobis_challenge_log_2019_csv"'

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


@app.route("/columns/", methods=['GET'])
@cross_origin()
def findColumn():
    columnList = datamodel.tables.find(table_name).columns
    columnName = []
    for x in columnList:
        columnName.append(x['name'])
    return json.dumps(columnName)


@app.route("/estimate/", methods=['GET'])
@cross_origin()
def estimate():
    selectedColumn = request.args.get('selectedColumn')
    epsilon = request.args.get('epsilon')
    number_of_values = request.args.get('number_of_values')
    recursion_depth = request.args.get('recursion_depth')
    other_query = PQL()
    other_query += PQLColumn(
        "ESTIMATE_CLUSTER_PARAMS( VARIANT (" + table_name + '."{}" ), {}, {}, {} )'.format(selectedColumn, epsilon,
                                                                                           number_of_values,
                                                                                           recursion_depth),
        name="MIN_PTS")
    other = datamodel.get_data_frame(other_query)
    minPTSList = other['MIN_PTS'].tolist()
    return json.dumps(minPTSList)


@app.route("/query/", methods=['GET'])
@cross_origin()
def query():
    selectedColumn = request.args.get('selectedColumn')
    minPTS = request.args.get('minPTS')
    epsilon = request.args.get('epsilon')
    customName = request.args.get('customName')
    query = PQL()
    query += PQLColumn(
        "CLUSTER_VARIANTS ( VARIANT ( " + table_name + '.{} ),{}, {} )'.format(selectedColumn, minPTS, epsilon),
        name="{}".format(customName))
    query += PQLColumn("VARIANT ( " + table_name + '.{} )'.format(selectedColumn), name="variant")
    query += PQLColumn(table_name + '."CASE"', name="caseId3")
    activity_column = datamodel._get_data_frame(query)
    result = activity_column.to_json(orient="records")
    return json.dumps(json.loads(result))


@app.route("/plot/", methods=['GET'])
@cross_origin()
def drawplot():
    selectedColumn = request.args.get('selectedColumn')
    epsilon = request.args.get('epsilon')
    minPTS = request.args.get('minPTS')
    numberList = request.args.get('numberList')
    customName = request.args.get("customName")
    query = PQL()
    query += PQLColumn(
        "CLUSTER_VARIANTS ( VARIANT ( " + table_name + '.{} ),{}, {} )'.format(selectedColumn, minPTS, epsilon),
        name="{}".format(customName))
    query += PQLColumn("VARIANT ( " + table_name + '.{} )'.format(selectedColumn), name="variant")
    query += PQLColumn(table_name + '."CASE"', name="caseId")
    query += PQLColumn(table_name + '."{}"'.format(selectedColumn), name="{}".format(customName))
    query += PQLColumn(table_name + '."START"', name="start")

    query += PQLFilter(
        "FILTER CLUSTER_VARIANTS ( VARIANT ( " + table_name + '.{} ),{}, {} )  IN ({})'.format(selectedColumn, minPTS,
                                                                                               epsilon, numberList))
    activity_column = datamodel._get_data_frame(query)
    df = activity_column[["caseId", "{}".format(customName), "start"]]
    df = pm4py.format_dataframe(df, case_id='caseId', activity_key='{}'.format(customName), timestamp_key='start')
    log = log_converter.apply(df)
    dfg = dfg_discovery.apply(log)
    gviz = dfg_visualization.apply(dfg)

    return send_file(dfg_visualization.view(gviz), mimetype='image/png')


@app.route("/compute/", methods=['GET'])
@cross_origin()
def comput():
    activity_table_case = '"mobis_challenge_log_2019_csv"."CASE"'
    activity_table_activity = '"mobis_challenge_log_2019_csv"."ACTIVITY"'
    activity_table_start = '"mobis_challenge_log_2019_csv"."START"'
    activity_table_end = '"mobis_challenge_log_2019_csv"."END"'
    activity_table_type = '"mobis_challenge_log_2019_csv"."TYPE"'
    activity_table_user = '"mobis_challenge_log_2019_csv"."USER"'
    activity_table_travel_start = '"mobis_challenge_log_2019_csv"."TRAVEL_START"'
    activity_table_travel_end = '"mobis_challenge_log_2019_csv"."TRAVEL_END"'
    activity_table_cost = '"mobis_challenge_log_2019_csv"."COST"'

    selectedColumn = request.args.get('selectedColumn')
    epsilon = request.args.get('epsilon')
    minPTS = request.args.get('minPTS')
    numberList = request.args.get('numberList')
    customName = "clusterID"
    query = PQL()

    query += PQLColumn(f"CLUSTER_VARIANTS ( VARIANT ({table_name}.{selectedColumn} ),{minPTS}, {epsilon} )",
                       name=f"{customName}")
    query += PQLColumn(f"VARIANT ( {table_name}.{selectedColumn})", name="variant")
    query += PQLColumn(activity_table_case, name="caseId")
    query += PQLColumn(activity_table_activity, name="activity")
    query += PQLColumn(activity_table_start, name="tableStart")
    query += PQLColumn(activity_table_end, name="tableEnd")
    query += PQLColumn(activity_table_type, name="type")
    query += PQLColumn(activity_table_user, name="user")
    query += PQLColumn(activity_table_travel_start, name="travelStart")
    query += PQLColumn(activity_table_travel_end, name="travelEnd")
    query += PQLColumn(activity_table_cost, name="cost")

    query += PQLFilter(
        "FILTER CLUSTER_VARIANTS ( VARIANT ( " + table_name + '.{} ),{}, {} )  IN ({})'.format(selectedColumn, minPTS,
                                                                                               epsilon, numberList))

    activity_column = datamodel._get_data_frame(query)

    df = activity_column.loc[:, 'caseId':'cost']

    costStd = df['cost'].std()
    costMean = df['cost'].mean(axis=0, skipna=True)
    activityUnique = df['activity'].nunique()
    user_unique = df['user'].nunique()
    activityTimeMean, activityTimeStd = timeDiffer(df['tableEnd'], df['tableStart'])
    travelTimeMean, travelTimeStd = timeDiffer(df['travelEnd'], df['travelStart'])
    data_set = {"activityUnique": activityUnique, "userUnique": user_unique, "costMean": costMean, "costStd": costStd,
                "activityTimeMean": activityTimeMean, "activityTimeStd": activityTimeStd,
                "travelTimeMean": travelTimeMean, "travelTimeStd": travelTimeStd
                }
    return data_set
