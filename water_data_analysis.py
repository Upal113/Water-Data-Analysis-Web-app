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
from PIL import Image
import io
import numpy as np
from sklearn.linear_model import LinearRegression

gc = gspread.service_account(filename='water-q-327318-b5ed18e63c87.json')
water_data_analysis = gc.open_by_key('1peyI2Dn2km2YaHT8wporv6f1tmBHWrI8maMUyJ2hNqg')
queued_reports = water_data_analysis.worksheet('Queued')
fixed_reports = water_data_analysis.worksheet('Fixed Reports')

st.set_page_config(page_title='Water Quality Control', page_icon='💧', layout='wide')

gc = gspread.service_account(filename='water-q-327318-65fff8bfd57b.json')
water_data_analysis = gc.open_by_key('1peyI2Dn2km2YaHT8wporv6f1tmBHWrI8maMUyJ2hNqg')
queued_reports = water_data_analysis.worksheet('Queued')
fixed_reports = water_data_analysis.worksheet('Fixed Reports')
water_data =  pd.DataFrame(water_data_analysis.worksheet('Input Data').get_all_records())
water_data['Temp']=pd.to_numeric(water_data['Temp'],errors='coerce')
water_data['D.O. (mg/l)']=pd.to_numeric(water_data['D.O. (mg/l)'],errors='coerce')
water_data['PH']=pd.to_numeric(water_data['PH'],errors='coerce')
water_data['B.O.D. (mg/l)']=pd.to_numeric(water_data['B.O.D. (mg/l)'],errors='coerce')
water_data['NITRATENAN N+ NITRITENANN (mg/l)']=pd.to_numeric(water_data['NITRATENAN N+ NITRITENANN (mg/l)'],errors='coerce')
water_data['TOTAL COLIFORM (MPN/100ml)Mean']=pd.to_numeric(water_data['TOTAL COLIFORM (MPN/100ml)Mean'],errors='coerce')
water_data['CONDUCTIVITY']=pd.to_numeric(water_data['CONDUCTIVITY'],errors='coerce')
def wqi_rating(wqi):
    if wqi<=45:
        return 'Good'
    elif 45<wqi<=60:
        return 'Fair'
    elif wqi>60:
        return 'Poor'
def get_table_download_link(df):
            """Generates a link allowing the data in a given panda dataframe to be downloaded
            in:  dataframe
            out: href string
            """
            csv = df.to_csv()
            b64 = base64.b64encode(csv.encode()).decode('utf-8')  # some strings <-> bytes conversions necessary here
            href = f'<a href="data:file/csv;base64,{b64}" download="output.csv">Download csv file</a>'
            return href

date_select_start = st.date_input(label="Select Starting Date", value=(dt.now()-timedelta(days=365)))
date_select_end = st.date_input(label="Select End Date")

start_time = st.time_input(label='Select Time')
end_time = st.time_input(label='Select End Time', value=(dt.now()-timedelta(hours=23)))
date_time_select_start = dt.combine(date_select_start, start_time)
date_time_select_end = dt.combine(date_select_end, end_time)
select_ph_range = st.slider(label='Please Select pH Range of the water', 
                            min_value=0,
                            max_value=14,
                            value=(0,14)
                            )
select_temp_range = st.slider(label='Please Select Temperature of the water', 
                            min_value=int(min(water_data['Temp']))-10,
                            max_value=int(max(water_data['Temp']))+5,
                            value=((int(min(water_data['Temp']))-10), (int(max(water_data['Temp']))+10))
                            )                            
location_selection = st.multiselect(label='Select Locations',
               options= water_data['Location'].unique().tolist(),
               default=water_data['Location'].unique().tolist())

join = '<h4><a href="mailto:upalkundu287@gmail.com">Contact Developer</a></h4>'
st.markdown(join, unsafe_allow_html=True)
water_data['Time Compare'] = pd.to_datetime(water_data['Time'])
filter = ((water_data['Location'].isin(location_selection)) & 
            (water_data['Time Compare']<=date_time_select_end) & 
            (water_data['Time Compare']>=date_time_select_start) &
            (water_data['PH'].between(*select_ph_range))&
            (water_data['Temp'].between(*select_temp_range))) 

water_data = water_data[filter]
water_data['npH']=water_data.PH.apply(lambda x: (100 if (8.5>=x>=7)  
                                 else(80 if  (8.6>=x>=8.5) or (6.9>=x>=6.8) 
                                      else(60 if (8.8>=x>=8.6) or (6.8>=x>=6.7) 
                                          else(40 if (9>=x>=8.8) or (6.7>=x>=6.5)
                                              else 0)))))
water_data['ndo']=water_data['D.O. (mg/l)'].apply(lambda x:(100 if (x>=6)  
                                 else(80 if  (6>=x>=5.1) 
                                      else(60 if (5>=x>=4.1)
                                          else(40 if (4>=x>=3) 
                                              else 0)))))
water_data['nco']=water_data['TOTAL COLIFORM (MPN/100ml)Mean'].apply(lambda x:(100 if (5>=x>=0)  
                                 else(80 if  (50>=x>=5) 
                                      else(60 if (500>=x>=50)
                                          else(40 if (10000>=x>=500) 
                                              else 0)))))
water_data['nbdo']=water_data['D.O. (mg/l)'].apply(lambda x:(100 if (3>=x>=0)  
                                 else(80 if  (6>=x>=3) 
                                      else(60 if (80>=x>=6)
                                          else(40 if (125>=x>=80) 
                                              else 0)))))
water_data['nec']=water_data['CONDUCTIVITY'].apply(lambda x:(100 if (75>=x>=0)  
                                 else(80 if  (150>=x>=75) 
                                      else(60 if (225>=x>=150)
                                          else(40 if (300>=x>=225) 
                                              else 0)))))
water_data['nna']=water_data['NITRATENAN N+ NITRITENANN (mg/l)'].apply(lambda x:(100 if (20>=x>=0)  
                                 else(80 if  (50>=x>=20) 
                                      else(60 if (100>=x>=50)
                                          else(40 if (200>=x>=100) 
                                              else 0))))) 
                                              
                                                                                           
water_data['wph']=water_data.npH * 0.165
water_data['wdo']=water_data.ndo * 0.281
water_data['wbdo']=water_data.nbdo * 0.234
water_data['wec']=water_data.nec* 0.009
water_data['wna']=water_data.nna * 0.028
water_data['wco']=water_data.nco * 0.281
water_data['wqi']=water_data.wph+water_data.wdo+water_data.wbdo+water_data.wec+water_data.wna+water_data.wco 
water_data['Wqi Rating'] = water_data['wqi'].apply(lambda x: wqi_rating(x))
st.title('Data : ')
st.dataframe(water_data.astype('str'))
water_data['Time Compare'] = pd.to_datetime(water_data['Time'])
col1, col2 = st.columns(2)
for location in water_data['Location'].unique().tolist():
    fig = px.scatter(water_data[water_data['Location']==location], x='Hour', y='PH', animation_frame='Dates', size='PH', 
                     color='PH',range_color=(0,14),
                title=f'Hourly statistics of pH of {location}')
    col1.plotly_chart(fig)
    fig = px.scatter(water_data[water_data['Location']==location], x='Hour', y='Temp', animation_frame='Dates', size='Temp', 
                     color='Temp',
                title=f'Hourly statistics of Temprature of {location}')
    col2.plotly_chart(fig)

for location in water_data['Location'].unique().tolist():
    fig = px.line(water_data[water_data['Location']==location], x='Hour', y='wqi', animation_frame='Dates',
                 range_y=(0,100),text='wqi',
                title=f'Hourly statistics of Water Quality Index of {location}')
    st.plotly_chart(fig)
     


st.title(body='Locations of operation')
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
st.title(body='Water Supply Chain')
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
fixed_data = pd.DataFrame(fixed_reports.get_all_records()).astype('str')[['Time',
 'Dates',
 'Hour',
 'Location',
 'Lat',
 'Lon',
 'Temp',
 'D.O. (mg/l)',
 'PH',
 'CONDUCTIVITY',
 'B.O.D. (mg/l)',
 'NITRATENAN N+ NITRITENANN (mg/l)',
 'FECAL COLIFORM (MPN/100ml)',
 'TOTAL COLIFORM (MPN/100ml)Mean',
 'Time',                                         
 'npH',
 'ndo',
 'nco',
 'nbdo',
 'nec',
 'wph',
 'wdo',
 'wbdo',
 'wec',
 'nna',
 'wna',
 'wco',
 'wqi',
 'Wqi Rating']]
queued_data = pd.DataFrame(queued_reports.get_all_records()).astype('str')[['Time',
 'Dates',
 'Hour',
 'Location',
 'Lat',
 'Lon',
 'Temp',
 'D.O. (mg/l)',
 'PH',
 'CONDUCTIVITY',
 'B.O.D. (mg/l)',
 'NITRATENAN N+ NITRITENANN (mg/l)',
 'FECAL COLIFORM (MPN/100ml)',
 'TOTAL COLIFORM (MPN/100ml)Mean',
 'Time',                                                                         
 'npH',
 'ndo',
 'nco',
 'nbdo',
 'nec',
 'wph',
 'wdo',
 'wbdo',
 'wec',
 'nna',
 'wna',
 'wco',
 'wqi',
 'Wqi Rating']]

st.title('Maping Of Problemetic Areas')

requring_solve = water_data[water_data['wqi']>=46]
st.title('Problematic Issues Requring Solve')
st.dataframe(requring_solve.astype('str'))
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

problem_list = requring_solve.astype('str').values.tolist()
for l in problem_list:
    if l not in fixed_data.values.tolist():
        if l in queued_data.values.tolist():
            st.warning('At ' + l[0] + ' In ' + l[3] + ' Water Quality Index Was ' + l[-2] +' Which Is '+ l[-1] + ' This Was Queued To Be Fixed Later.')
            fixed = st.checkbox(label='It is Fixed', key=problem_list.index(l))
            if fixed:
                r = l.append(str(dt.now()))
                fixed_reports.append_row(l,value_input_option='RAW',insert_data_option='INSERT_ROWS', table_range='A1:K1')
        else:  
            
            st.error('At ' + l[0] + ' In ' + l[3] + ' Water Quality Index Was ' + l[-2] +' Which Is '+ l[-1])
            queued = st.checkbox(label='Queued', key=problem_list.index(l))
            if queued:
                r = l.append(str(dt.now()))
                queued_reports.append_row(l, value_input_option='RAW',insert_data_option='INSERT_ROWS', table_range='A1:K1')
            fixed = st.checkbox(label='Fixed', key=problem_list.index(l))
            if fixed:
                r = l.append(str(dt.now()))
                fixed_reports.append_row(l,value_input_option='RAW',insert_data_option='INSERT_ROWS', table_range='A1:K1')



water_data =  pd.DataFrame(water_data_analysis.worksheet('Input Data').get_all_records())
water_data['Time Compare'] = pd.to_datetime(water_data['Time'])
water_data['Temp']=pd.to_numeric(water_data['Temp'],errors='coerce')
water_data['D.O. (mg/l)']=pd.to_numeric(water_data['D.O. (mg/l)'],errors='coerce')
water_data['PH']=pd.to_numeric(water_data['PH'],errors='coerce')
water_data['B.O.D. (mg/l)']=pd.to_numeric(water_data['B.O.D. (mg/l)'],errors='coerce')
water_data['NITRATENAN N+ NITRITENANN (mg/l)']=pd.to_numeric(water_data['NITRATENAN N+ NITRITENANN (mg/l)'],errors='coerce')
water_data['TOTAL COLIFORM (MPN/100ml)Mean']=pd.to_numeric(water_data['TOTAL COLIFORM (MPN/100ml)Mean'],errors='coerce')
water_data['CONDUCTIVITY']=pd.to_numeric(water_data['CONDUCTIVITY'],errors='coerce')
water_data['npH']=water_data.PH.apply(lambda x: (100 if (8.5>=x>=7)  
                                 else(80 if  (8.6>=x>=8.5) or (6.9>=x>=6.8) 
                                      else(60 if (8.8>=x>=8.6) or (6.8>=x>=6.7) 
                                          else(40 if (9>=x>=8.8) or (6.7>=x>=6.5)
                                              else 0)))))
water_data['ndo']=water_data['D.O. (mg/l)'].apply(lambda x:(100 if (x>=6)  
                                 else(80 if  (6>=x>=5.1) 
                                      else(60 if (5>=x>=4.1)
                                          else(40 if (4>=x>=3) 
                                              else 0)))))
water_data['nco']=water_data['TOTAL COLIFORM (MPN/100ml)Mean'].apply(lambda x:(100 if (5>=x>=0)  
                                 else(80 if  (50>=x>=5) 
                                      else(60 if (500>=x>=50)
                                          else(40 if (10000>=x>=500) 
                                              else 0)))))
water_data['nbdo']=water_data['D.O. (mg/l)'].apply(lambda x:(100 if (3>=x>=0)  
                                 else(80 if  (6>=x>=3) 
                                      else(60 if (80>=x>=6)
                                          else(40 if (125>=x>=80) 
                                              else 0)))))
water_data['nec']=water_data['CONDUCTIVITY'].apply(lambda x:(100 if (75>=x>=0)  
                                 else(80 if  (150>=x>=75) 
                                      else(60 if (225>=x>=150)
                                          else(40 if (300>=x>=225) 
                                              else 0)))))
water_data['nna']=water_data['NITRATENAN N+ NITRITENANN (mg/l)'].apply(lambda x:(100 if (20>=x>=0)  
                                 else(80 if  (50>=x>=20) 
                                      else(60 if (100>=x>=50)
                                          else(40 if (200>=x>=100) 
                                              else 0))))) 
                                              
                                                                                           
water_data['wph']=water_data.npH * 0.165
water_data['wdo']=water_data.ndo * 0.281
water_data['wbdo']=water_data.nbdo * 0.234
water_data['wec']=water_data.nec* 0.009
water_data['wna']=water_data.nna * 0.028
water_data['wco']=water_data.nco * 0.281
water_data['wqi']=water_data.wph+water_data.wdo+water_data.wbdo+water_data.wec+water_data.wna+water_data.wco 
water_data['Wqi Rating'] = water_data['wqi'].apply(lambda x: wqi_rating(x))
water_data['Date Compare'] = pd.to_datetime(water_data['Dates'])
for location in water_data['Location'].unique().tolist():
    st.write(f'Predicted Forecasting for the next 20 days of {location}')
    location_data = water_data[water_data['Location']==location].groupby(['Dates', 'Location'])['wqi'].mean().reset_index()
    location_data.set_index('Dates')
    location_data['Date Num'] = np.arange(1, len(location_data)+1)
    location_data
    formula = LinearRegression()
    x = location_data['Date Num'].values.reshape(-1,1)
    y = location_data['wqi'].values.reshape(-1,1)
    formula.fit(x,y)
    for date in range(len(location_data)+2, len(location_data)+20):
        st.write(str(water_data['Date Compare'].tolist()[-1]+ timedelta(days=date)))
        predicts = formula.predict([[date]])
        st.write(float(predicts))

for location in water_data['Location'].unique().tolist():
    st.write(f'Predicted Ph for the next 20 days of {location}')
    location_data = water_data[water_data['Location']==location].groupby(['Dates', 'Location'])['PH'].mean().reset_index()
    location_data.set_index('Dates')
    location_data['Date Num'] = np.arange(1, len(location_data)+1)
    location_data
    formula = LinearRegression()
    x = location_data['Date Num'].values.reshape(-1,1)
    y = location_data['PH'].values.reshape(-1,1)
    formula.fit(x,y)
    for date in range(len(location_data)+2, len(location_data)+20):
        st.write(str(water_data['Date Compare'].tolist()[-1]+ timedelta(days=date)))
        predicts = formula.predict([[date]])
        st.write(float(predicts))
