from flask import Flask, render_template, request, jsonify, send_file,flash, redirect, url_for, make_response
import os
import io
import json
import pandas as pd
from flask_cors import CORS
import sqlite3
import numpy as np
import geojson
import math
from shapely.geometry import Point, shape

from werkzeug.utils import secure_filename
from ast import literal_eval

app = Flask(__name__)
CORS(app)
# cwd = os.getcwd()

# refer to Tariff-API-data-prep for preparing the backend data

@app.route('/')
def base():
    return jsonify('Welcome to CEEM''s API centre! Please select an API. For example: /electricity-tariffs/network')
    # return jsonify(cwd)

# https://ceem-api.herokuapp.com/BusinessLoadProfiles/utilities
@app.route('/BusinessLoadProfiles/<bus_type>')
def bus_load_profiles(bus_type):
    data_file = pd.read_csv(os.path.join('application', 'Bus_LP.csv')) 
    data_file = data_file[['TS',bus_type]].copy()
    data_file = data_file.rename(columns={bus_type:'kWh'})
    data_file = data_file.to_json(orient='records')
    return jsonify(data_file)

# https://ceem-api.herokuapp.com/BusinessLoadProfiles_List
@app.route('/BusinessLoadProfiles_List')
def bus_load_profiles_list():
    data_file = pd.read_csv(os.path.join('application', 'Bus_LP.csv')) 
    col = [col for col in data_file.columns if col != 'TS']
    return jsonify(col)

# https://ceem-api.herokuapp.com/BusinessLoadProfiles_List_temp
# to be removed after Nabeen confirms swicth to new list
@app.route('/BusinessLoadProfiles_List_temp')
def bus_load_profiles_list_temp():
    data_file = pd.read_csv(os.path.join('application', 'Bus_LP_temp.csv')) 
    col = [col for col in data_file.columns if col != 'TS']
    return jsonify(col)


@app.route('/Tariffs/AllTariffs')
def Alltariffs():
    with open(os.path.join('application', 'AllTariffs_Retail.json')) as data_file:
        data_loaded = json.load(data_file)
        return jsonify(data_loaded)


# This part is for previous versions of retail tariffs or the version list
# try "/v1", "/v2", etc or "/versions" to track the version list
@app.route('/electricity-tariffs/retail/<version>')
def retail_tariff_v(version):
    with open(os.path.join('application', 'AllTariffs_Retail_{}.json'.format(version))) as data_file:
        data_loaded = json.load(data_file)
        return jsonify(data_loaded)


#  the version compatible with Tariff tool (nb: this name will change to retail and the previous one will be removed. It is not removed now because it is being used by SunSpoT
# https://ceem-api.herokuapp.com/electricity-tariffs/retail
@app.route('/electricity-tariffs/retail')
def retail_tariff():
    with open(os.path.join('application', 'AllTariffs_Retail.json')) as data_file:
        data_loaded = json.load(data_file)
        return jsonify(data_loaded)

@app.route('/electricity-tariffs/default_for_sunspot')
def default_for_sunspot():
    with open(os.path.join('application', 'Tariffs_Retail_default_sunspot.json')) as data_file:
        data_loaded = json.load(data_file)
        return jsonify(data_loaded)

#  For SunSpoT project. We will remove this later
@app.route('/elec-tariffs/retail')
def retail_tariff_SunSpoT():
    with open(os.path.join('application', 'AllTariffs_Retail_SunSpoT.json')) as data_file:
        data_loaded = json.load(data_file)
        return jsonify(data_loaded)

#  Network tariffs:
# This part is for previous versions of Network tariffs or the version list
# try "/v1", "/v2", etc or "/versions" to track the version list
@app.route('/electricity-tariffs/network/<version>')
def network_tariff_v(version):
    with open(os.path.join('application', 'AllTariffs_Network_{}.json'.format(version))) as data_file:
        data_loaded = json.load(data_file)
        return jsonify(data_loaded)


#  most recent version
@app.route('/electricity-tariffs/network')
def network_tariff():
    with open(os.path.join('application', 'AllTariffs_Network.json')) as data_file:
        data_loaded = json.load(data_file)
        return jsonify(data_loaded)

@app.route('/electricity-tariffs/large-commercial-tariffs')
def large_commercial_tariffs():
    with open(os.path.join('application', 'AllTariffs_LargeCommercial.json')) as data_file:
        data_loaded = json.load(data_file)
        return jsonify(data_loaded)

#  weather data from NASA Power
# https://ceem-api.herokuapp.com/weather/20220101/20220201/-32/150
@app.route('/weather/<start_date>/<end_date>/<lat>/<long>')
def weather_data(start_date, end_date, lat, long):
    lat = round(2 * float(lat)) / 2
    long = round(2 * float(long)) / 2
    with sqlite3.connect(os.path.join('application', 'nasa_power.db')) as con:
        lat_long_list = pd.read_sql_query(con=con, sql='select distinct lat, long from data')
        lat_long_list2 = lat_long_list.copy()
        lat_long_list2['Lat'] = abs(lat_long_list['Lat'] - lat)
        lat_long_list2['Long'] = abs(lat_long_list['Long'] - long)
        lat_long_list2['both'] = lat_long_list2['Lat'] + lat_long_list2['Long']
        ind_min = lat_long_list2['both'].idxmin()
        lat_new = lat_long_list.loc[ind_min, 'Lat']
        long_new = lat_long_list.loc[ind_min, 'Long']
        data_w = pd.read_sql_query(con=con, sql='select TS, CDD, HDD from data where TS >= {} and TS <= {}'
                                                ' and lat == {} and long == {}'.format(start_date, end_date, str(lat_new), str(long_new)))
        data_w = data_w.drop_duplicates(subset='TS', keep='last')
        data_w2 = data_w.to_json(orient='records')
        return jsonify(data_w2)

#  weather data from NASA Power THIS PART IS FOR TESTING THE AUTOMATIC UPDATE. WHEN TEST IS DONE IT WILL BE REMOVED
# https://ceem-api.herokuapp.com/weather2/20220101/20220201/-32/150
@app.route('/weather2/<start_date>/<end_date>/<lat>/<long>')
def weather_data2(start_date, end_date, lat, long):
    lat = round(2 * float(lat)) / 2
    long = round(2 * float(long)) / 2
    with sqlite3.connect(os.path.join('application', 'nasa_power2.db')) as con:
        lat_long_list = pd.read_sql_query(con=con, sql='select distinct lat, long from data')
        lat_long_list2 = lat_long_list.copy()
        lat_long_list2['Lat'] = abs(lat_long_list['Lat'] - lat)
        lat_long_list2['Long'] = abs(lat_long_list['Long'] - long)
        lat_long_list2['both'] = lat_long_list2['Lat'] + lat_long_list2['Long']
        ind_min = lat_long_list2['both'].idxmin()
        lat_new = lat_long_list.loc[ind_min, 'Lat']
        long_new = lat_long_list.loc[ind_min, 'Long']
        data_w = pd.read_sql_query(con=con, sql='select TS, CDD, HDD from data where TS >= {} and TS <= {}'
                                                ' and lat == {} and long == {}'.format(start_date, end_date, str(lat_new), str(long_new)))
        data_w = data_w.drop_duplicates(subset='TS', keep='last')
        data_w2 = data_w.to_json(orient='records')
        return jsonify(data_w2)

 # Finding the dnsp
#  https://ceem-api.herokuapp.com/dnsp/-32/150
@app.route('/dnsp/<lat>/<long>')
def find_dnsp(lat, long):
    # path_to_file = 'dnsp_finder/latest-distribution-boundaries.geojson'
    # with open(os.path.join('application', 'latest-distribution-boundaries.geojson')) as f:
    with open(os.path.join('application', 'distribution_boundaries_2022.geojson')) as f:
        gj = geojson.load(f)

    s_list = gj['features']

    p = Point([float(long), float(lat)])

    found_dnsp = False
    dnsp_name = ''
    dnsp_index = -1

    for i in range(0, len(s_list)):
        if (shape(s_list[i].geometry).contains(p)):
            found_dnsp = True
            dnsp_index = i
            break
    if (found_dnsp):
        dnsp_name = s_list[dnsp_index].properties.get("network")
    if dnsp_name in ['Actewagl', 'Evoenergy']:
        dnsp_name = 'EvoEnergy'
    if dnsp_name == '':
        dnsp_name = 'Off-grid'
    return jsonify(dnsp_name)

 # Finding the AER benchmarking (based on the AER Bnechmarking for energy consumptions)
#  https://ceem-api.herokuapp.com/AERBenchmarking/2010
@app.route('/AERBenchmarking/<postcode>')
def find_aer_bm(postcode):
    AER = pd.read_csv(os.path.join('application', 'AERBenchmark.csv'))
    cols = AER.columns.drop(['Season','Postcode','Climate Zone','State Zone Season','Household size'])
    AER[cols] = AER[cols].apply(pd.to_numeric, errors='coerce').fillna(0)
    AER['Postcode'] = AER['Postcode'].astype(int)
    PC = float(postcode)
    # Use QLD for NT and WA for now
    if PC <2000:
        PC=4000
    elif (PC<7000) & (PC>=6000):
        PC=4000
    AER['Distance']=abs(AER['Postcode']-PC)
    AER_ = AER[AER['Distance']==AER['Distance'].min()].reset_index(drop=True)
    if AER_.shape[0]>4:
        AER_ = AER_[AER_['Postcode']==AER_['Postcode'].min()].reset_index(drop=True)
    # AER_
    AER_2 = AER_.to_json(orient='records')
    return jsonify(AER_2)

# #  Tariff Docs
@app.route('/tariff-source/<tariff_id>')
def tariff_source(tariff_id):
    if tariff_id.startswith('TR'):
        return str('We have obtained the retail tariffs from EnergyMadeEasy Website (https://www.energymadeeasy.gov.au/). Please refer to this website and search for this tariff for more information.')
    else:
        pdf_to_tariff_map = pd.read_csv(os.path.join('application', 'PDFs', 'pdf_to_tariff_map.csv'))
        # print(tariff_id)
        # return(pdf_to_tariff_map.loc[pdf_to_tariff_map['Tariff ID'] == str(tariff_id)]['PDF'].values[0])
        try:
            return send_file(os.path.join('PDFs', str(pdf_to_tariff_map.loc[pdf_to_tariff_map['Tariff ID'] == tariff_id]['PDF'].values[0]) + '.pdf'))
        except:
            return str('There is no document for this tariff.')

# uploading load profiles
UPLOAD_FOLDER = '/uploads'
ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/lp_upload', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            return redirect(url_for('uploaded_file',
                                    filename=filename))
        file_read = pd.read_csv(file)
        # need to check the column numbers and decide about the type
        if file_read.shape[1]>10:
            NEM12_1 = file_read.copy()
            Chunks = np.where((NEM12_1[NEM12_1.columns[0]] == 200) | (NEM12_1[NEM12_1.columns[0]] == 900))[0]
            NEM12_2 = pd.DataFrame()
            for i in range(0, len(Chunks) - 1):
                if NEM12_1.iloc[Chunks[i], 4].startswith('E'):
                    this_part = NEM12_1.iloc[Chunks[i] + 1: Chunks[i + 1], :].copy()
                    this_part = this_part[this_part[this_part.columns[0]] == 300].copy()
                    this_part2 = this_part.iloc[:, 2:50]
                    this_part2 = this_part2.astype(float)
                    if (this_part2[
                            this_part2 < 0.01].count().sum() / this_part2.count().sum()) < 0.3:  # assume for controlled load more 30% of data points are zero
                        NEM12_2 = NEM12_1.iloc[Chunks[i] + 1: Chunks[i + 1], :].copy()
                        NEM12_2.reset_index(inplace=True, drop=True)

            NEM12_2 = NEM12_2[NEM12_2[NEM12_2.columns[0]] == 300].copy()
            NEM12_2[NEM12_2.columns[1]] = NEM12_2[NEM12_2.columns[1]].astype(int).astype(str)
            NEM12_2 = NEM12_2.iloc[:, 0:49]
            col_name = [i for i in range(0, 49)]
            NEM12_2.columns = col_name
            Nem12 = NEM12_2.iloc[:, 1:50].melt(id_vars=[NEM12_2.columns[1]], var_name="HH", value_name="kWh")  # it was 49.. need to check if Dan manually changed it
            Nem12['HH'] = Nem12['HH'] - 1
            Nem12['kWh'] = Nem12['kWh'].astype(float)
            Nem12['Datetime'] = pd.to_datetime(Nem12[NEM12_2.columns[1]], format='%Y%m%d') + pd.to_timedelta(Nem12['HH'] * 30, unit='m')
            Nem12.sort_values('Datetime', inplace=True)
            # Nem12_ = Nem12.groupby(['Datetime','HH']).sum().reset_index()
            Nem12.reset_index(inplace=True, drop=True)
            sample_load = Nem12[['Datetime', 'kWh']].copy()
            sample_load.rename(columns={'Datetime': 'TS'}, inplace=True)
        elif file_read.shape[1] ==2:
           # It's two column and just need to remove the 29Feb and make it 15 min
            sample_load = file_read.copy()
            sample_load.columns = ['TS', 'kWh']
            sample_load['TS'] = pd.to_datetime(sample_load['TS'],format="%d/%m/%Y %H:%M")
        else:
            # file_read_2 = pd.read_csv(file)
            file_read_2 = file_read.copy()
            # check the column name if it has report.. for webgraph..
            Report_cols = [col for col in file_read_2.columns if 'REPORT' in col]
            kWh_Con = [col for col in file_read_2.columns if 'KWH_CON' in col]
            if len(Report_cols)==2:
                # it is webgraph
                file_read_3 = file_read_2[['REPORT_DATE', 'REPORT_TIME',kWh_Con[0]]].copy()
                file_read_3['R3'] = pd.to_datetime(file_read_3['REPORT_DATE'], format="%d/%m/%Y", errors='coerce')
                file_read_3 = file_read_3.dropna()
                file_read_3['TS'] = file_read_3['R3'] + pd.to_timedelta(file_read_3['REPORT_TIME'].str[0:2].astype(int), unit='H') + pd.to_timedelta(
                    file_read_3['REPORT_TIME'].str[3:].astype(int), unit='m')
                sample_load = file_read_3[['TS', kWh_Con[0]]].copy()
                sample_load.columns = ['TS', 'kWh']
        # sample_load['TS'] = pd.to_datetime(sample_load['TS'])
        sample_load = sample_load.set_index('TS')
        sample_load = sample_load.resample('30min', closed='right', label='right').sum()
        sample_load = sample_load.reset_index()
        sample_load = sample_load[sample_load['TS'].dt.normalize() != '2020-02-29'].copy()

        sample_load['TS'] = sample_load['TS'].dt.strftime("%d/%m/%Y %H:%M")

        resp = make_response(sample_load.to_csv(index=False))
        resp.headers["Content-Disposition"] = "attachment; filename=export.csv"
        resp.headers["Content-Type"] = "text/csv"
        return resp
    return '''
    <!doctype html>
    <title>Upload your load profile</title>
    <h1>Convert your load profile to standard format (NEM12 and WebGraph)</h1>
    <form method=post enctype=multipart/form-data>
      <input type=file name=file>
      <input type=submit value=Upload>
    </form>
    '''


# Calculates the shading array. For documentation and examples, please refer to shading_calculator.py
@app.route('/calculate_shading/<pv_panel_group>/<shading_boxes>/<shading_cylinders>')
# def calculate_shading(pv_panel_group, shading_boxes, shading_cylinders):
#     shading_array = shading_calculator.generate_shading_arrays_for_pv_panel_group(pv_panel_group, shading_boxes, shading_cylinders)
#     return jsonify(shading_array)



def calculate_shading(pv_panel_group, shading_boxes, shading_cylinders,
                                               max_grid_space=0.30, buffer_from_edge=0.30):
    """
    Generates a shading array for a pv panel group by determining the fraction of the pv panel group that
    will be shaded by the boxes and cylinders at a set zenith and azimuth angles in 5 degree increments range from 0-90
    and 0-355 degrees respectively. The 3D co-ordinate system is defined such that the y-axis runs true
    south-north, with the north direction being positive, the x-axis runs west-east with the east direction being
    positive, and the z-axis runs down-up, with the up direction being positive. All distance values are metres.

    Note:

    To improve the runtime of this function it is assumed that all shading objects start at or below the level of the
    lowest part of the PV panel group, and shading objects don't have 'over hangs'. This allows the shading algorithm
    to stop checking azimuth angles if they are not shaded at a greater zenith angle.

    Examples:

    >>> panel_group = [(0, 0, 0), (2, 0, 0), (2, -1.415, 1.415), (0, -1.415, 1.415)]
    >>> boxes = [{'points': [(0, 0), (2, 0), (2, 2), (0, 2)], 'height': 2}]
    >>> cylinders = []
    >>> shading_array = generate_shading_arrays_for_pv_panel_group(panel_group, boxes, cylinders)

    >>> shading_array['s0'][:10]
    [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

    >>> len(shading_array['s0'])
    72

    >>> len(shading_array.keys())
    91

    >>> list(shading_array.keys())[:10]
    ['s0', 's1', 's2', 's3', 's4', 's5', 's6', 's7', 's8', 's9']


    Args:
        pv_panel_group: list[tuple] definition of a panel group is a list of four tuples where each tuple is the x,y,z
            co-ordinates of a corner of the panel group in 3D space. The order in which the corners are provided should
            be such that adjacent corners are consecutive in the list, and the first and last corners are adjacent.
            Note that this function assumes each panel group is a rectangle or square. An example set of pv
            panel groups would be [[(0, 0, 0), (6, 0, 0), (6, -1.4, 1.4), (0, -1.4, 1.4)]], in this case there is one
            group which is 6 metres long, running west-east, approx 2-wide, at a tilt of 45 degrees facing north.
        shading_boxes: list[dict] Each dictionary in the list defines a 3D box that could shade a panel
            group. Each box is a rectangular prism defined by 4 points that describe the corners of the prism
            (tuples with x,y values), and one height value, the points must be in order such that adjacent corners are
            consecutive in list, and the first and last corners are adjacent. An example box dictionary is
            {'points': [(0,0), (0,1), (2,1), (2,0)], 'height': 3]}.
        shading_cylinders: list[dict] Each dictionary in the list defines a 3D Cylinder that could shade a
            panel group. Each cylinder is described by a centre point, a radius, and a height value. An example
            cylinder dictionary is {'centre': (0,0), 'radius': 1, 'height': 3]}.

    Returns: dict{list} The keys in the dictionary are
        's0', 's1' . . . 's90' where the number after 's' is the zenith angle for the values in the corresponding list.
        Each list in the dictionary has 72 values, which correspond to 72 azimuth angles in 5 degree increments, i.e.
        azimuth angles from 0 to 355 degrees. Each value in the list is a float between zero and one that specifies the
        fraction of the panel group that will be shaded if the sun was at the corresponding angle. An example list for
        two panel groups would look like
        [{'s0': [0.0, 0.0, ... 0.0],  's1': [0.0, 0.0, ... 0.0], ... 's90': [0.0, 0.0, ... 0.0]},
         {'s0': [0.0, 0.0, ... 0.0],  's1': [0.0, 0.0, ... 0.0], ... 's90': [0.0, 0.0, ... 0.0]}]
    """
    pv_panel_group = literal_eval(pv_panel_group)
    shading_boxes = json.loads(shading_boxes)
    shading_cylinders = json.loads(shading_cylinders)

    points = generate_grid_of_points_on_panel_group(pv_panel_group, max_grid_space=max_grid_space,
                                                    buffer_from_edge=buffer_from_edge, precision=3)
    box_sides = generate_box_sides(shading_boxes)
    shading_arrays = generate_shading_arrays_for_points(points, box_sides, shading_cylinders)
    shading_array = aggregate_shading_arrays(shading_arrays)
    shading_array = format_shading_array(shading_array)
    return shading_array


def generate_grid_of_points_on_panel_group(panel_group, max_grid_space, buffer_from_edge, precision=3):
    """
    Generate a set of points in 3D space in a grid on the panel group, the grid spacing is determined such that the
    minimum number of points are used while keeping the spacing below max_grid_space. The grid starts within
    the panel group at the distance buffer_from_edge.

    This function was written with assistance from ChatGPT4.

    Examples:

    This is a simple example where the panel group lies on the x-y plane. The panel group size and the buffer mean
    the grid points will be space 1 m apart, starting at 1 running to 9 on the x-axis, and starting at 1 and running
    to 19 on the y-axis.

    >>> rectangle_coords = [(0, 0, 0), (10, 0, 0), (10, 20, 0), (0, 20, 0)]
    >>> max_space = 1.0
    >>> buffer = 1.0
    >>> points = generate_grid_of_points_on_panel_group(rectangle_coords, max_space, buffer)
    >>> points[0]
    (1.0, 1.0, 0.0)

    >>> points[-1]
    (9.0, 19.0, 0.0)

    Args:
        panel_group: list[tuple] A list of four tuples where the tuple is the x,y,z co-ordinates of a corner of the
            panel group in 3D space. The order in which the corners are provided should be such that adjacent corners
            are consecutive in the list, and the first and last corners are adjacent. Note that this function assumes
            each panel group is a rectangle or square. An example pv panel group would be
            [(0, 0, 0), (6, 0, 0), (6, -1.4, 1.4), (0, -1.4, 1.4)], in this case the group is 6 metres long, running
            west-east, approx 2-wide, at a tilt of 45 degrees facing north.
        max_grid_space: float the maximum allowed space between points on the grid.
        buffer_from_edge: float the distance in from the edge to start the grid.
        precision: int the number of decimal places in the co-ordinates of the grid points
    """

    assert len(panel_group) == 4, "There must be 4 coordinates to define a rectangle."

    def interpolate(a, b, t):
        return a * (1 - t) + b * t

    def distance(a, b):
        return np.sqrt(np.sum((b - a) ** 2))

    p0, p1, p2, p3 = np.array(panel_group)

    # Calculate buffer factors based on distance and buffer
    buffer_factor_edge1 = buffer_from_edge / distance(p0, p1)
    buffer_factor_edge2 = buffer_from_edge / distance(p0, p3)

    # Calculate inner rectangle coordinates
    inner_p0 = interpolate(p0, p1, buffer_factor_edge1) + interpolate(p0, p3, buffer_factor_edge2) - p0
    inner_p1 = interpolate(p1, p0, buffer_factor_edge1) + interpolate(p1, p2, buffer_factor_edge2) - p1
    inner_p2 = interpolate(p2, p3, buffer_factor_edge1) + interpolate(p2, p1, buffer_factor_edge2) - p2
    inner_p3 = interpolate(p3, p2, buffer_factor_edge1) + interpolate(p3, p0, buffer_factor_edge2) - p3

    # Calculate grid size based on max_space
    edge1_length = distance(inner_p0, inner_p1)
    edge2_length = distance(inner_p0, inner_p3)

    n = max(2, int(np.ceil(edge1_length / max_grid_space)) + 1)
    m = max(2, int(np.ceil(edge2_length / max_grid_space)) + 1)

    points = []

    for i in range(n):
        for j in range(m):
            t1 = i / (n - 1)
            t2 = j / (m - 1)

            # Find two points on opposite sides of the inner rectangle that are in line with the final grid point
            # we are calculating the position of.
            temp1 = interpolate(inner_p0, inner_p1, t1)
            temp2 = interpolate(inner_p3, inner_p2, t1)

            # Interpolate between the two points we just found to find the final grid point.
            points.append(tuple(interpolate(temp1, temp2, t2).round(precision)))

    return points


def generate_box_sides(shading_boxes):
    """
    Reformat the shading box data so each side is defined separately.

    Args:
        shading_boxes: list[dict] Each dictionary in the list defines a 3D box that could shade a panel
            group. Each box is a rectangular prism defined by 4 points that describe the corners of the prism
            (tuples with x,y values), and one height value, the points must be in order such that adjacent corners are
            consecutive in list, and the first and last corners are adjacent. An example box dictionary is
            {'points': [(0,0), (0,1), (2,1), (2,0)], 'height': 3]}.

    Returns:
        list[dict] list of side definitions. Each side is defined using two x,y points, the height of
        the box, and the pre-computed vector normal of the plane the side sits on. An example dictionary would be
        {'points': [(0,0), (0,1)], 'height': 3, 'vector_normal': (0, 1, 0)]}
    """
    shading_box_sides = []
    for box in shading_boxes:
        shading_box_sides.append(compose_box_side_definition(box, 0, 1))
        shading_box_sides.append(compose_box_side_definition(box, 1, 2))
        shading_box_sides.append(compose_box_side_definition(box, 2, 3))
        shading_box_sides.append(compose_box_side_definition(box, 3, 0))
    return shading_box_sides


def compose_box_side_definition(box, point_1, point_2):
    side_points = [box['points'][point_1], box['points'][point_2]]
    side_vector_normal = calculate_vector_normal(
        tuple(box['points'][point_1]) + (0,),
        tuple(box['points'][point_2]) + (0,),
        tuple(box['points'][point_1]) + (box['height'],)
    )
    return {'points': side_points, 'height': box['height'], 'vector_normal': side_vector_normal}


def calculate_vector_normal(p1, p2, p3):
    """
    Compute the normal vector of a plane defined by three points

    Args:
        p1: tuple
        p2: tuple
        p3: tuple

    Returns:
        numpy array that represents the normal vector
    """
    # Convert points to numpy arrays
    p1, p2, p3 = np.array(p1), np.array(p2), np.array(p3)

    # Compute two vectors that lie on the plane
    v1 = p2 - p1
    v2 = p3 - p1

    # Compute the cross product of v1 and v2 to get the normal vector
    normal = np.cross(v1, v2)

    # Normalize the normal vector
    normal = normal / np.linalg.norm(normal)

    return tuple(normal)


def generate_shading_arrays_for_points(points, shading_sides_boxes, shading_cylinders):
    """
    For a list of points in 3D space generate an array of 0s and 1s for each point to specify if the point would be
    shaded by the 3D objects if the sun was at the given angles. A 0 indicates the point would not be shaded and 1
    indicates it would be shaded.

    Examples:

    Args:
        points: list[tuple(int)] a list of  x, y, z co-ordinates of the points. All values are in metres.
        shading_sides_boxes: list[dict] list of side definitions. Each side is defined using two x,y points, the height
            of the box, and the pre-computed vector normal of the plane the side sits on. An example dictionary would be
            {'points': [(0,0), (0,1)], 'height': 3, 'vector_normal': (0, 1, 0)]}
        shading_cylinders: list[dict] a list of dictionaries. Each dictionary defines a 3D Cylinder that could shade the
            point. Each is cylinder is described by a centre point, a radius, and a height value. An example cylinder
            dictionary is {'centre': (0,0), 'radius': 1, 'height': 3]}. All values are in metres.

    Returns: list[pd.DataFrame] where each pd.DataFrame specifies the shading of one point on the surface of the PV
        panel group. Each pd.DataFrame has the columns azimuth, zenith, and shaded. azimuth and zenith
        are the angles of a line to check the shading for in degrees. shaded is a boolean value specifying if the line
        on that angle hits a shading object. The angles checked are all the combinations of the 72 azimuth angles
        starting at 0 through to 355 in 5 degree increments, and 91 zenith angles starting a 0 through to 90, in 1
        degree increments.
    """
    angle_vectors = generate_vectors_of_angles_in_shading_array_format()
    shading_arrays = []
    for point in points:
        shading_arrays.append(generate_shading_array_for_point(point, shading_sides_boxes, shading_cylinders,
                                                               angle_vectors.copy()))
    return shading_arrays


def generate_vectors_of_angles_in_shading_array_format():
    """
    Create a dictionary in the shading array format where the values are tuples specify the direction of the angle
    as an x, y, z vector.

    This function was written with assistance from ChatGPT4.

    Example:

    >>> generate_vectors_of_angles_in_shading_array_format()

    Returns: pd.DataFrame with the columns azimuth, zenith, zenith_search_group, and vector. The azimuth and zenith
        angles are all the combinations of the 72 azimuth angles starting at 0 through to 355 in 5 degree increments,
        and 91 zenith angles starting a 0 through to 90, in 1 degree increments. The zenith_search_group search group is
        the decade of the zenith angle (i.e. 9 for 90, 8 for 83 etc.), this is used to group the angles to check the
        shading in batches. The vector is the 3D vector specifying the direction of line this is used later when
        checking where a line traveling at the given angle intercepts other objects in 3D space.

    """

    data_rows = []
    for zenith in range(0, 91, 5):
        zenith_radians = math.radians(zenith)
        zenith_search_group = zenith // 10
        for azimuth in range(0, 360, 5):
            azimuth_radians = math.radians(azimuth)
            x = math.sin(zenith_radians) * math.sin(azimuth_radians)
            y = math.sin(zenith_radians) * math.cos(azimuth_radians)
            z = math.cos(zenith_radians)
            data_rows.append((azimuth, zenith, zenith_search_group, (x, y, z)))

    angles_and_vectors = pd.DataFrame(data_rows, columns=['azimuth', 'zenith', 'zenith_search_group', 'vector'])

    return angles_and_vectors


def aggregate_shading_arrays(shading_arrays):
    """
    Takes a list of shading arrays for a set of points in 3D space and aggregates them by finding the fraction of the
    points that are shaded at each angle.

    Args:
        shading_arrays:
            list[pd.DataFrame] where each pd.DataFrame specifies the shading of one point on the surface of
            the PV panel group. Each pd.DataFrame has the columns azimuth, zenith, and shaded. azimuth and zenith
            are the angles of a line to check the shading for in degrees. shaded is a boolean value specifying if the line
            on that angle hits a shading object. The angles checked are all the combinations of the 72 azimuth angles
            starting at 0 through to 355 in 5 degree increments, and 91 zenith angles starting a 0 through to 90, in 1
            degree increments.

    Returns:
        pd.DataFrame with the columns azimuth, zenith, and shaded. The azimuth and zenith angles are all the
        combinations of the 72 azimuth angles starting at 0 through to 355 in 5 degree increments,
        and 91 zenith angles starting a 0 through to 90, in 1 degree increments. shaded is the fraction of points that
        are shaded at the given angle.

    """
    number_of_points = len(shading_arrays)
    shading_arrays = pd.concat(shading_arrays)
    shading_arrays = shading_arrays.loc[:, ['azimuth', 'zenith', 'shaded']]
    shading_arrays['shaded'] = np.where(shading_arrays['shaded'], 1, 0)
    shading_array = shading_arrays.groupby(['azimuth', 'zenith'], as_index=False)['shaded'].sum()
    shading_array['shaded'] = shading_array['shaded'] / number_of_points
    return shading_array


def format_shading_array(shading_array):
    """
    Changes the format of the shading array from a data frame to a dictionary.

    Args:
        shading_array:
            pd.DataFrame with the columns azimuth, zenith, and shaded. The azimuth and zenith angles are all the
            combinations of the 72 azimuth angles starting at 0 through to 355 in 5 degree increments,
            and 91 zenith angles starting a 0 through to 90, in 1 degree increments. shaded is the fraction of points
            that  are shaded at the given angle.

    Returns: dict{list} The keys in the dictionary are 's0', 's1' . . . 's90' where the number after 's' is the zenith
        angle for the values in the corresponding list. Each list in the dictionary has 72 values, which correspond to
        72 azimuth angles in 5 degree increments, i.e. azimuth angles from 0 to 355 degrees. Each value in the list is
        a float between zero and one that specifies the fraction of the panel group that will be shaded if the sun was
        at the corresponding angle. An example list for two panel groups would look like
        [{'s0': [0.0, 0.0, ... 0.0],  's1': [0.0, 0.0, ... 0.0], ... 's90': [0.0, 0.0, ... 0.0]},
         {'s0': [0.0, 0.0, ... 0.0],  's1': [0.0, 0.0, ... 0.0], ... 's90': [0.0, 0.0, ... 0.0]}]
    """
    re_formatted_array = {}
    zenith_groups = shading_array.groupby(['zenith'], as_index=False)
    for group, data in zenith_groups:
        if type(group) == tuple:
            group = group[0]
        data = data.sort_values('azimuth')
        re_formatted_array['s' + str(group)] = list(data['shaded'])
    return re_formatted_array


def generate_shading_array_for_point(point, shading_boxes_sides, shading_cylinders, angles):
    """
    For a given point in 3D space generates an array of 0s and 1s to specify if the point would be shaded by the
    3D objects if the sun was at the given angles. A 0 indicates the point would not be shaded and 1 indicates it
    would be shaded.

    Examples:

    Args:
        point: tuple(int) the x, y, z co-ordinates of the point. All values are in metres.
        shading_boxes_sides: list[dict] list of side definitions. Each side is defined using two x,y points, the height
            of the box, and the pre-computed vector normal of the plane the side sits on. An example dictionary would be
            {'points': [(0,0), (0,1)], 'height': 3, 'vector_normal': (0, 1, 0)}
        shading_cylinders: list[dict] a list of dictionaries. Each dictionary defines a 3D Cylinder that could shade the
            point. Each is cylinder is described by a centre point, a radius, and a height value. An example cylinder
            dictionary is {'centre': (0,0), 'radius': 1, 'height': 3}. All values are in metres.
        angles: pd.DataFrame with the columns azimuth, zenith, zenith_search_group, and vector. The azimuth and zenith
            angles are all the combinations of the 72 azimuth angles starting at 0 through to 355 in 5 degree
            increments, and 91 zenith angles starting a 0 through to 90, in 1 degree increments. The zenith_search_group
            search group is the decade of the zenith angle (i.e. 9 for 90, 8 for 83 etc.), this is used to group the
            angles to check the shading in batches. The vector is the 3D vector specifying the direction of line this
            is used later when checking where a line traveling at the given angle intercepts other objects in 3D space.

    Returns: pd.DataFrame with the columns azimuth, zenith, and shaded. The azimuth and zenith angles are all the
        combinations of the 72 azimuth angles starting at 0 through to 355 in 5 degree increments,
        and 91 zenith angles starting a 0 through to 90, in 1 degree increments. The shaded column specifies if a
        line at the given angle intercepts one of the shading objects.
    """
    # Grouping is based on the tens digit of the azimuth, so 90 is group 9, and 83 is group 8 etc. This means if we
    # sort by descending we will check just everything at the horizontal first, and then check angles in batches
    # of 10 degree increments.
    angles = angles.sort_values('zenith_search_group', ascending=False)
    azimuths_to_keep_checking = None
    shading_results = []
    for zenith_search_group, angle_set in angles.groupby('zenith_search_group', as_index=False, sort=False):
        # Only check azimuth angles if they were shaded at zenith angle closer to the horizontal. This greatly
        # improves the efficiency of the shading calculation.
        if azimuths_to_keep_checking is not None:
            angle_set = angle_set[angle_set['azimuth'].isin(azimuths_to_keep_checking)].copy()
        # If no azimuth angles in the last set were shaded we can stop looking for angles that shaded.
        if angle_set.empty:
            break
        # Check the shading of the angles we need to check.
        angle_set['shaded'] = angle_set.apply(
            lambda x: check_if_angle_shaded(point, x['vector'], shading_boxes_sides, shading_cylinders), axis=1)
        # Find the azimuth angles that were shaded in the last set (only considering the minimum zenith angle in the
        # last set)
        min_angle_in_group = angle_set['zenith'].min()
        azimuths_to_keep_checking = \
            angle_set[(angle_set['shaded']) & (angle_set['zenith'] == min_angle_in_group)]['azimuth']
        shading_results.append(angle_set)

    shading_results = pd.concat(shading_results)

    # Combine results with original full set of angles, assume not shaded for any angles the search algorithm didn't
    # check.
    angles = pd.merge(angles, shading_results.loc[:, ['zenith', 'azimuth', 'shaded']], how='left',
                      on=['azimuth', 'zenith'])
    angles = angles.fillna(False)

    return angles


def check_if_angle_shaded(point, vector, shading_boxes_sides, shading_cylinders):
    """
    Checks if the line defined by the point and the vector intercepts with any of the specified shading objects. Note,
    checks that intercept occurs in the positive direction of the line.

    Args:
        point: tuple(float) the x, y, z co-ordinates of the point. All values are in metres.
        vector: tuple(float) the direction of the line specified by its vector components in the x, y, z directions
        shading_boxes_sides: list[dict] list of side definitions. Each side is defined using two x,y points, the height
            of the box, and the pre-computed vector normal of the plane the side sits on. An example dictionary would be
            {'points': [(0,0), (0,1)], 'height': 3, 'vector_normal': (0, 1, 0)}
        shading_cylinders: list[dict] a list of dictionaries. Each dictionary defines a 3D Cylinder that could shade the
            point. Each is cylinder is described by a centre point, a radius, and a height value. An example cylinder
            dictionary is {'centre': (0,0), 'radius': 1, 'height': 3}. All values are in metres.

    Returns: boolean value specify if the line intercepts a shading object.
    """
    line = {'point': point, 'vector': vector}
    for side in shading_boxes_sides:
        if check_if_line_goes_through_box_side(line, side):
            return True
    for cylinder in shading_cylinders:
        if check_if_line_intercepts_cylinder(line, cylinder):
            return True
    else:
        return False


def check_if_line_goes_through_box_side(line, side):
    """
    Check if a line goes through the side of box. By first checking if the line goes through the plane that the side
    of the box sits on. If its does, finding the point that the line intersects the plane. Lastly checking if that
    point is within the bounds of that defined the edges of the side of the box.

    Examples:

    Args:
        line: dict{tuple} A line defined using a point and vector. The dictionary would be
            {'point': (x, y, z), 'vector':(a, b, c)}. Where x, y, z are the co-ordinates of a point that the line
            passes through. and a, b, and c are the components of the vector in the x, y, z direction. So an example
            line that passes through the origin and heads directly north at altitude of 45 degrees would be
            {'point': (0, 0, 0), 'vector':(0, 1, 1)}
        side: dict{} definition of side using two x,y points, the height of the box, and the pre-computed vector normal
            of the plane the side sits on. An example dictionary would be
            {'points': [(0,0), (0,1)], 'height': 3, 'vector_normal': (0, 1, 0)]}

    Returns: Boolean
    """
    plane = {'point': tuple(side['points'][0]) + (side['height'],), 'vector_normal': side['vector_normal']}
    intercept = find_line_intercept_with_plane(line, plane)
    if intercept == 'parallel':
        return False
    elif intercept == 'coincident':
        return True
    else:
        if check_if_point_of_intercept_is_in_positive_direction_of_vector(line, intercept):
            return check_if_intercept_point_within_bounds_of_side(intercept, side)
        else:
            return False


def find_line_intercept_with_plane(line, plane):
    """
    Finds where a line in 3D space intercepts a plane in 3D space.

    This function was written with assistance from ChatGPT4.

    If the line is coincident with the plane the string 'coincident' is returned, if the line is parallel to the plane ]
    the string 'parallel' is returned, if line intercepts the plane at a point the (x, y, z) value is returned.

    Maths behind the function:

    Given a line defined by a point (x0, y0, z0) and a vector <a, b, c> we can describe the line in parametric form
    using the equations:

        x = x0 + a*t
        y = y0 + b*t
        z = z0 + c*t

    Given the vector normal of plane <A, B, C> and a point on the plane (x1, y1, z1) we can describe the plane using
    the equation

        A(x - x1) + B(y - y1) + C(z - z1) = 0

    To check if the line we can substitute the equations for the point into the equation for the line and solve for t.

        A(x0 + a*t - x1) + B(y0 + b*t - y1) + C(z0 + c*t - z1) = 0

        t = (A*(x1 - x0) + B*(y1 - y0) + C*(z1 - z0)) / (A*a + B*b + C*c)

    If the denominator (A*a + B*b + C*c) the line is either parallel to the plane or coincident with the plane. If the
    point defining the line is also on the plane then the line is coincident with plane, otherwise the line is
    parallel to the plane.

    If the denominator does not equal zero, then we can find a value for t, and then the x, y, z co-orindates
    of the point of interception using the parametric form of the line.

    Examples:

    Define a line that one metre down the y-axis and goes straight up, and a plane that runs along the x and z axis.
    Then attempt to compute the intercept, the results should be 'parallel'.

    >>> line0 = {'point': (0, 1, 0), 'vector': (0, 0, 1)}

    >>> plane0 = {'point': (0, 0, 0), 'vector_normal': (0, 1, 0)}

    >>> find_line_intercept_with_plane(line0, plane0)
    'parallel'

    Now change the line to go through the origin, the result should be 'coincident'.

    >>> line0 = {'point': (0, 0, 0), 'vector': (0, 0, 1)}

    >>> find_line_intercept_with_plane(line0, plane0)
    'coincident'

    Now change the line back to the first starting point, but slow it back towards the plane at 45 degrees. The
    result should be an intercept 1 metre above the origin.

    >>> line0 = {'point': (0, 1, 0), 'vector': (0, -1, 1)}

    >>> find_line_intercept_with_plane(line0, plane0)
    (0.0, 0.0, 1.0)

    Args:
        line: dict{tuple} A line defined using a point and vector. The dictionary would be
            {'point': (x, y, z), 'vector':(a, b, c)}. Where x, y, z are the co-ordinates of a point that the line
            passes through. and a, b, and c are the components of the vector in the x, y, z direction. So an example
            line that passes through the origin and heads directly north at altitude of 45 degrees would be
            {'point': (0, 0, 0), 'vector':(0, 1, 1)}
        plane: dict{} definition of plane using an x,y,z point and the vector normal of the plane. An example dictionary
            would be {'point': (0, 0, 3), 'vector_normal': (0, 1, 0)]}

    Returns: str or tuple e.g. 'coincident', 'parallel', or (x, y, z)
    """

    #t0 = time()
    # Unpack inputs into variable values used in documentation
    # Point on line
    x0 = line['point'][0]
    y0 = line['point'][1]
    z0 = line['point'][2]
    # Vector of line
    a = line['vector'][0]
    b = line['vector'][1]
    c = line['vector'][2]
    # Point on plane
    x1 = plane['point'][0]
    y1 = plane['point'][1]
    z1 = plane['point'][2]
    # Vector normal of plane
    A = plane['vector_normal'][0]
    B = plane['vector_normal'][1]
    C = plane['vector_normal'][2]
    #times['unpacking'] += time() - t0

    #t0 = time()
    solution_numerator = (A * (x1 - x0) + B * (y1 - y0) + C * (z1 - z0))
    solution_denominator = (A * a + B * b + C * c)
    #times['solution_numerator and solution_denominator'] += time() - t0

    #t0 = time()
    # Check if line is coincident or parallel to plane.
    if solution_denominator == 0:
        # Check if line is coincident to plane.
        if A * (x0 - x1) + B * (y0 - y1) + C * (z0 - z1) == 0:
            return 'coincident'
        else:
            return 'parallel'
    #times['parallel checks'] += time() - t0

    #t0 = time()
    t = solution_numerator / solution_denominator

    x = x0 + a * t
    y = y0 + b * t
    z = z0 + c * t
    #times['compute point'] += time() - t0

    return x, y, z


def check_if_intercept_point_within_bounds_of_side(point, side):
    """
    Check if a point in 3D space is within the bounds of the side of a box. Assumes the side of the box is vertical.

    Maths behind function:

    Because the sides of the box are vertical we can check if the x,y co-ordinates of the intercept are within the
    x,y limits of the side separately to consider the height of the intercept.

    If the two points defining the side of the box are (x0, y0) and (x1, y1), and the points defining the intercept are
    (xI, yI), then intercept is within the bounds of the box if x1 is between x0 and x1, and yI is between y0 and y1.
    Which, without know if x0 > x1 or not, we can check by testing is xI is closer to x0 and x1 than x0 is to x1, and
    the same for y.

    Then we just additionally check if the z of the intercept is less than the height of the side.

    Examples:

    Example where point is within side.

    >>> side_1 = {'points': [(0, 10), (10, 0)], 'height': 3}
    >>> point_1 = (5, 5, 2)
    >>> check_if_intercept_point_within_bounds_of_side(point_1, side_1)
    True

    Example where point is too high

    >>> point_2 = (5, 5, 3.1)
    >>> check_if_intercept_point_within_bounds_of_side(point_2, side_1)
    False

    Example where point is outside the side because it's too far in the x direction

    >>> point_3 = (11, -1, 2)
    >>> check_if_intercept_point_within_bounds_of_side(point_3, side_1)
    False

    Example where point is outside the side because it's not far enough in the x direction

    >>> point_4 = (-1, 11, 2)
    >>> check_if_intercept_point_within_bounds_of_side(point_4, side_1)
    False

    Args:
        point: tuple the x, y, co-ordinates of the point.
        side: dict{} definition of side using two x,y points, and the height of the box. An example dictionary would be
            {'points': [(0,0), (0,1)], 'height': 3]}
    """
    xI = point[0]
    yI = point[1]
    zI = point[2]
    x0 = side['points'][0][0]
    y0 = side['points'][0][1]
    x1 = side['points'][1][0]
    y1 = side['points'][1][1]

    # x-axis distance between points.
    xI2x0 = abs(xI - x0)
    xI2x1 = abs(xI - x1)
    x02x1 = abs(x0 - x1)

    # y-axis distance between points.
    yI2y0 = abs(yI - y0)
    yI2y1 = abs(yI - y1)
    y02y1 = abs(y0 - y1)

    # We only need to check x or y values, however, when x0 and x1 or y0 and y1 are very close we can run into
    # precision errors, therefore, we check using whichever axis has the greatest distance between points.
    if x02x1 > y02y1:
        if xI2x0 <= x02x1 and xI2x1 <= x02x1:
            xy_values_within_side = True
        else:
            xy_values_within_side = False
    else:
        if yI2y0 <= y02y1 and yI2y1 <= y02y1:
            xy_values_within_side = True
        else:
            xy_values_within_side = False

    if side['height'] >= zI >= 0:
        z_value_within_side = True
    else:
        z_value_within_side = False

    return xy_values_within_side and z_value_within_side


def check_if_line_intercepts_cylinder(line, cylinder):
    """"
    Checks if a line intercepts a cylinder. Note: also checks if the intercept is in the positive direction of travel
    of the line.

    This function was written with assistance from ChatGPT4.

    Args:
        line: dict{tuple} A line defined using a point and vector. The dictionary would be
            {'point': (x, y, z), 'vector':(a, b, c)}. Where x, y, z are the co-ordinates of a point that the line
            passes through. and a, b, and c are the components of the vector in the x, y, z direction. So an example
            line that passes through the origin and heads directly north at altitude of 45 degrees would be
            {'point': (0, 0, 0), 'vector':(0, 1, 1)}
        cylinder: dict that defines a 3D Cylinder that could shade the point. The cylinder is described by a
            centre point, a radius, and a height value. An example cylinder dictionary is
            {'centre': (0,0), 'radius': 1, 'height': 3}. All values are in metres.

    """
    # define variables for convenience
    x0, y0, z0 = line['point']
    a, b, c = line['vector']
    h, k = cylinder['centre']

    # Coefficients for the quadratic equation At^2 + Bt + C = 0
    A = a ** 2 + b ** 2
    B = 2 * a * (x0 - h) + 2 * b * (y0 - k)
    C = (x0 - h) ** 2 + (y0 - k) ** 2 - cylinder['radius'] ** 2

    # Compute the discriminant
    discriminant = B ** 2 - 4 * A * C

    if discriminant < 0:
        # If discriminant is less than 0, the line does not intersect the cylinder.
        return False
    else:
        # Otherwise, solve for t
        t1 = (-B - np.sqrt(discriminant)) / (2 * A)
        t2 = (-B + np.sqrt(discriminant)) / (2 * A)

        # Check the z-coordinates of the intersection points
        x1 = x0 + t1 * a
        x2 = x0 + t2 * a
        y1 = y0 + t1 * b
        y2 = y0 + t2 * b
        z1 = z0 + t1 * c
        z2 = z0 + t2 * c

        intercept_1 = (x1, y1, z1)
        intercept_2 = (x2, y2, z2)

        z_min = 0
        z_max = cylinder['height']

        if check_if_point_of_intercept_is_in_positive_direction_of_vector(line, intercept_1):
            if z_min <= z1 <= z_max:
                return True

        if check_if_point_of_intercept_is_in_positive_direction_of_vector(line, intercept_2):
            if z_min <= z2 <= z_max:
                return True

    # If none of the conditions are met, return False
    return False


def check_if_point_of_intercept_is_in_positive_direction_of_vector(line, point):
    """
    Checks if the intercept point is in the positive direction of travel for the line.

    This function was written with assistance from ChatGPT4.

    Args:
        line: dict{tuple} A line defined using a point and vector. The dictionary would be
            {'point': (x, y, z), 'vector':(a, b, c)}. Where x, y, z are the co-ordinates of a point that the line
            passes through. and a, b, and c are the components of the vector in the x, y, z direction. So an example
            line that passes through the origin and heads directly north at altitude of 45 degrees would be
            {'point': (0, 0, 0), 'vector':(0, 1, 1)}
        point: tuple(float) The intercept point.
    """
    # Calculate vector components from the point defining the line to intercept point.
    v1x = point[0] - line['point'][0]
    v1y = point[1] - line['point'][1]
    v1z = point[2] - line['point'][2]

    # Check the direction of each component of v1 is the same as the line vector.
    if line['vector'][0] != 0:
        check_x = (v1x / line['vector'][0]) >= 0
    else:
        check_x = True

    if line['vector'][1] != 0:
        check_y = (v1y / line['vector'][1]) >= 0
    else:
        check_y = True

    if line['vector'][2] != 0:
        check_z = (v1z / line['vector'][2]) >= 0
    else:
        check_z = True

    return check_x and check_y and check_z
