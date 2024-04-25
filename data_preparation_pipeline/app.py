
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

selected_neighborhoods = {}
with st.sidebar.expander("Neighborhoods Color Legend"):
    for neighborhood, color in colors.items():
        # Checkbox per neighbourhood 
        is_selected = st.checkbox(f"{neighborhood}", key=f"chk_{neighborhood}")
        selected_neighborhoods[neighborhood] = is_selected
        st.markdown(f"<span style='display: inline-block; width: 12px; height: 12px; background: {color};'></span>", unsafe_allow_html=True)

filtered_data = sampled_data[sampled_data['neighbourhood'].isin([neighborhood for neighborhood, selected in selected_neighborhoods.items() if selected])]


# Creation of a map visualization of Barcelona
m = folium.Map(location=[41.3879, 2.1699], zoom_start=12)
for row in filtered_data.collect():
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



# Mostrar el mapa
folium_static(m)





