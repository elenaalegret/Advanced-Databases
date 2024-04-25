
##################################
### Map4Tourism - Tourism Tool ###
##################################

# Imports
import streamlit as st
import folium
from streamlit_folium import folium_static
import numpy as np
import base64
from DataPreparationPipeline import *


def get_base64_of_bin_file(bin_file):
    with open(bin_file, 'rb') as f:
        data = f.read()
    return base64.b64encode(data).decode()



st.write("""
    <div style="display:flex;align-items:center;">
        <img src="data:image/png;base64,{}" width="110">
        <h1 style="margin-left:10px;">BCN Map4Tourism</h1>
        
    </div>
""".format(get_base64_of_bin_file("images/logo.png")), unsafe_allow_html=True)
st.write("Welcome! Choose your neighborhood 🏘️ and explore local restaurants alongside crime rate \n statistics for a more informed experience. 😊🍽️📊")



# !!!!!!Attention!!!!!_____________________________________________________________________________________
# Limited computational resources may restrict rendering capabilities locally 
# Additional resources would enable processing of larger datasets.
# --> Constarint less apartments for visualization in local:  num_samples/1000 -- only 10% of the total data
# --> If you have resources descoment the indicated line
num_samples = st.sidebar.slider("Percentage of Airbnb Locations", min_value=1, max_value=100, value=1) # Sample the Airbnb dataset
#sampled_data = df_airbnb.sample(withReplacement=False, fraction=num_samples/100, seed=42) # MORE RESOURCES
sampled_data = df_airbnb.sample(withReplacement=False, fraction=num_samples/1000, seed=42) # LESS RESOURCES
#__________________________________________________________________________________________________________

with st.sidebar.expander("Neighborhoods Color Legend"):
    for neighborhood, color in colors.items():
        st.markdown(f"<span style='display: inline-block; width: 12px; height: 12px; background: {color};'></span> {neighborhood}", unsafe_allow_html=True)

# Creation of a map visualization of Barcelona
m = folium.Map(location=[41.3879, 2.1699], zoom_start=12)

# Add markers for each point in the sample
#for row in sampled_data.collect():
    
    #folium.Marker(location=[row['latitude'], row['longitude']], popup=f"{description}", tooltip=f"{row['Name']}").add_to(m)

for row in sampled_data.collect():
    neighbourhood = row['neighbourhood']
    location = [row['latitude'], row['longitude']]
    marker_color = colors.get(neighbourhood, 'gray')  
    description = row['property_type'] + '\n\n' + 'Price ' + str(row['price']) + " €"

    # Crear el marcador con el color especificado
    folium.Marker(
        location=location,
        popup=f"{row['Name']}",
        tooltip=f"{description}",
        icon=folium.Icon(color=marker_color, icon='home', prefix='fa')
    ).add_to(m)


st.markdown(f'''
<div style="
    border-radius: 10px;
    border: 2px solid #ff9832;
    padding: 15px;
    margin-top: 5px;
    margin-bottom: 5px;
    font-size: 16px;
    color: #ff9832;
    background-color: #ffffff;
    box-shadow: 2px 2px 12px rgba(0,0,0,0.1);">
    <b> Displayed Apartments </b> {sampled_data.count()}
</div>
''', unsafe_allow_html=True)



# Mostrar el mapa
folium_static(m)


"""
selected_neighborhoods = {}

with st.sidebar.expander("Neighborhoods Color Legend"):
    for neighborhood, color in colors.items():
        # Creamos un checkbox para cada barrio y guardamos el estado en el diccionario
        is_selected = st.checkbox(f"{neighborhood}", key=f"chk_{neighborhood}")
        selected_neighborhoods[neighborhood] = is_selected
        # Mostrar el color del barrio junto al checkbox
        st.markdown(f"<span style='display: inline-block; width: 12px; height: 12px; background: {color};'></span>", unsafe_allow_html=True)

# Filtrar el DataFrame basado en la selección de barrios
filtered_data = df_airbnb[df_airbnb['neighbourhood'].isin([neighborhood for neighborhood, selected in selected_neighborhoods.items() if selected])]
"""


