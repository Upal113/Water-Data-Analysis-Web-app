import pandas as pd
import numpy as np
from datetime import date, timedelta
from datetime import datetime as dt
import gspread
from pkg_resources import require
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import base64
gc = gspread.service_account(filename='water-q-327318-65fff8bfd57b.json')
water_data_analysis = gc.open_by_key('1peyI2Dn2km2YaHT8wporv6f1tmBHWrI8maMUyJ2hNqg')
queued_reports = water_data_analysis.worksheet('Queued')
fixed_reports = water_data_analysis.worksheet('Fixed Reports')


st.set_page_config(page_title='Water Data Analysis', page_icon='💧', layout='wide')

gc = gspread.service_account(filename='water-q-327318-65fff8bfd57b.json')
water_data_analysis = gc.open_by_key('1peyI2Dn2km2YaHT8wporv6f1tmBHWrI8maMUyJ2hNqg')
queued_reports = water_data_analysis.worksheet('Queued')
fixed_reports = water_data_analysis.worksheet('Fixed Reports')
water_data =  pd.DataFrame(water_data_analysis.worksheet('Input Data').get_all_records())
def check_ph_quality(pH):
    if pH>=6.5 and pH<=8.5:
        return "Good"
    elif pH>=5 and pH<6.5:
        return "Slightly Acidic"
    elif pH>8.5 and pH<=14:
        return "Water is not so good"
    elif pH>=5:
        return "This water is Acidic" 
def get_table_download_link(df):
            """Generates a link allowing the data in a given panda dataframe to be downloaded
            in:  dataframe
            out: href string
            """
            csv = df.to_csv()
            b64 = base64.b64encode(csv.encode()).decode('utf-8')  # some strings <-> bytes conversions necessary here
            href = f'<a href="data:file/csv;base64,{b64}" download="output.csv">Download csv file</a>'
            return href

date_select_start = st.date_input(label="Select Starting Date")
date_select_end = st.date_input(label="Select End Date")

start_time = st.time_input(label='Select Time', value=(dt.now()-timedelta(hours=23)))
end_time = st.time_input(label='Select End Time')
date_time_select_start = dt.combine(date_select_start, start_time)
date_time_select_end = dt.combine(date_select_end, end_time)
select_ph_range = st.slider(label='Please Select pH Range of the water', 
                            min_value=0,
                            max_value=14,
                            value=(0,14)
                            )
select_temp_range = st.slider(label='Please Select Temperature of the water', 
                            min_value=int(min(water_data['Temprature']))-10,
                            max_value=int(max(water_data['Temprature']))+5,
                            value=((int(min(water_data['Temprature']))-10), (int(max(water_data['Temprature']))+10))
                            )                            
location_selection = st.multiselect(label='Select Locations',
               options= water_data['Location'].unique().tolist(),
               default=water_data['Location'].unique().tolist())

join = '<h4><a href="mailto:upalkundu287@gmail.com">Contact Developer</a></h4>'
st.markdown(join, unsafe_allow_html=True)

water_data['Water Quality By Ph'] = water_data['Ph'].apply(lambda x: check_ph_quality(x))
water_data['Time Compare'] = pd.to_datetime(water_data['Time'])
filter = ((water_data['Location'].isin(location_selection)) & 
            (water_data['Time Compare']<=date_time_select_end) & 
            (water_data['Time Compare']>=date_time_select_start) &
            (water_data['Ph'].between(*select_ph_range))&
            (water_data['Temprature'].between(*select_temp_range))) 

water_data = water_data[filter]
water_data['Water Quality By Ph'] = water_data['Ph'].apply(lambda x: check_ph_quality(x))
st.title('Data : ')
st.dataframe(water_data)
st.markdown(get_table_download_link(df=water_data), unsafe_allow_html=True)
water_data['Time Compare'] = pd.to_datetime(water_data['Time'])

for location in water_data['Location'].unique().tolist():
    fig = px.scatter(water_data[water_data['Location']==location], x='Hour', y='Ph', animation_frame='Dates', size='Ph', color='Ph',range_color=(0,14),
                facet_row='Location',
                title=f'Hourly statistics of pH of {location}')
    st.plotly_chart(fig)



fig = go.Figure(go.Scattermapbox(
        lat=water_data['Lat'],
        lon=water_data['Lon'],
        mode='markers+text',
        marker=go.scattermapbox.Marker(
            size=20
        ),
        text=water_data['Location'],
    ))

fig.update_layout(
    
    mapbox=dict(
        accesstoken='pk.eyJ1Ijoia3VuZHUiLCJhIjoiY2s4bzN0Nmt4MTR6aDNqbzJoMGI5cWp4byJ9.oXdKRoD0eXu_qynttt3wSw',
        bearing=0,
        style='mapbox://styles/kundu/cku2p8eqr38y618o441s7hf62',
        center=go.layout.mapbox.Center(
            lat=23.746502168908656,
            lon=90.36662599205701
        ),
        pitch=0,
        zoom=11.2
    )
)
st.plotly_chart(fig)

fig = go.Figure(go.Scattermapbox(
    mode = "markers+lines+text",
    lon = water_data['Lon'],
    lat = water_data['Lat'],
     marker=go.scattermapbox.Marker(
            size=20
        ),
        text=water_data['Location']))
fig.update_layout(
    
    mapbox=dict(
        accesstoken='pk.eyJ1Ijoia3VuZHUiLCJhIjoiY2s4bzN0Nmt4MTR6aDNqbzJoMGI5cWp4byJ9.oXdKRoD0eXu_qynttt3wSw',
        bearing=0,
        style='mapbox://styles/kundu/cku2p8eqr38y618o441s7hf62',
        center=go.layout.mapbox.Center(
            lat=23.746502168908656,
            lon=90.36662599205701
        ),
        pitch=0,
        zoom=11
    )
)
st.plotly_chart(fig)
fixed_data = pd.DataFrame(fixed_reports.get_all_records()).astype('str')[['Time','Dates','Hour','Location','Lat','Lon','Ph','Temprature','Water Quality By Ph', 'Date']]
queued_data = pd.DataFrame(queued_reports.get_all_records()).astype('str')[['Time','Dates','Hour','Location','Lat','Lon','Ph','Temprature','Water Quality By Ph', 'Date']]

st.title('Maping Of Problemetic Areas')

requring_solve = water_data[(water_data['Water Quality By Ph'] != 'Good')]
st.title('Problematic Issues Requring Solve')
st.dataframe(requring_solve)
st.markdown(get_table_download_link(df=requring_solve), 
            unsafe_allow_html=True)
st.title('Total Problems Detected Between'+ str(date_select_start) + ' And '+ str(date_select_end)+ '  ' + str(len(requring_solve)))
fig = go.Figure(go.Scattermapbox(
    mode = "markers+lines+text",
    lon = requring_solve['Lon'],
    lat = requring_solve['Lat'],
     marker=go.scattermapbox.Marker(
            size=20,
            color='red'
        ),
        text=requring_solve['Location']))
fig.update_layout(
    
    mapbox=dict(
        accesstoken='pk.eyJ1Ijoia3VuZHUiLCJhIjoiY2s4bzN0Nmt4MTR6aDNqbzJoMGI5cWp4byJ9.oXdKRoD0eXu_qynttt3wSw',
        bearing=0,
        style='mapbox://styles/kundu/cku2p8eqr38y618o441s7hf62',
        center=go.layout.mapbox.Center(
            lat=23.746502168908656,
            lon=90.36662599205701
        ),
        pitch=0,
        zoom=11.2
    )
)
st.plotly_chart(fig)
i=0
j=0
problem_list = requring_solve.astype('str').values.tolist()
for l in problem_list:
    if l not in fixed_data.values.tolist():
        if l in queued_data.values.tolist():
            j = j+1
            st.warning('At ' + l[0] + ' In ' + l[3] + ' Water Ph Was ' + l[6] + ' Which Indicates ' + l[8] + ' This Was Queued To Be Fixed Later.')
            fixed = st.checkbox(label='It is Fixed', key=problem_list.index(l))
            if fixed:
                r = l.append(str(dt.now()))
                fixed_reports.append_row(l,value_input_option='RAW',insert_data_option='INSERT_ROWS', table_range='A1:K1')
        else:  
            i = i + 1
            st.error('At ' + l[0] + ' In ' + l[3] + ' Water Ph Was ' + l[6] + ' Which Indicates ' + l[8])
            queued = st.checkbox(label='Queued', key=problem_list.index(l))
            if queued:
                r = l.append(str(dt.now()))
                queued_reports.append_row(l, value_input_option='RAW',insert_data_option='INSERT_ROWS', table_range='A1:K1')
            fixed = st.checkbox(label='Fixed', key=problem_list.index(l))
            if fixed:
                r = l.append(str(dt.now()))
                fixed_reports.append_row(l,value_input_option='RAW',insert_data_option='INSERT_ROWS', table_range='A1:K1')



