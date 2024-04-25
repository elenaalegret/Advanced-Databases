
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
num_samples = st.sidebar.slider("Percentage of Locations Displayed", min_value=1, max_value=100, value=1)
#sampled_data = df_airbnb.sample(withReplacement=False, fraction=num_samples/100, seed=42) # MORE RESOURCES
sampled_data = df_airbnb.sample(withReplacement=False, fraction=num_samples/1000, seed=42) # LESS RESOURCES
sampled_locations = df_locations.sample(withReplacement=False, fraction=num_samples/100, seed=42) 
#__________________________________________________________________________________________________________


# The user can choose the price range
# Filter apartments based on the selected price range
price_max = st.sidebar.slider("Maximum Price", min_value=0, max_value=int(df_airbnb.select(max("price")).first()[0]), value=50)
sampled_data = sampled_data.filter((sampled_data['price'] < price_max))


# Selection of neighbourhoods to visualize
selected_neighborhoods = {}
with st.sidebar.expander("Neighborhoods"):
    for neighborhood, color in colors.items():
        # Checkbox per neighbourhood 
        is_selected = st.checkbox(f"{neighborhood}", key=f"chk_{neighborhood}")
        selected_neighborhoods[neighborhood] = is_selected
        st.markdown(f"<span style='display: inline-block; width: 12px; height: 12px; background: {color};'></span>", unsafe_allow_html=True)

filtered_data = sampled_data[sampled_data['neighbourhood'].isin([neighborhood for neighborhood, selected in selected_neighborhoods.items() if selected])]
filtered_locations = sampled_locations[sampled_locations['neighbourhood'].isin([neighborhood for neighborhood, selected in selected_neighborhoods.items() if selected])]

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

for row in filtered_locations.collect():
    folium.Marker(
        location=[row['latitude'], row['longitude']],
        popup=f"{row['type']}",  # Popup con nombre y tipo
        tooltip=f"{row['name']}",
        icon=folium.Icon(color=colors.get(row['neighbourhood'], 'gray'), icon=location_icons.get(row['type']))
    ).add_to(m)

# Display the number of apartments & Locations
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
    <b> Displayed Apartments </b> {filtered_data.count()}
</div>
''', unsafe_allow_html=True)

st.markdown(f'''
<div style="
    border-radius: 10px;
    border: 2px solid #a26464;
    padding: 15px;
    margin-top: 5px;
    margin-bottom: 5px;
    font-size: 16px;
    color: #a26464;
    background-color: #ffffff;
    box-shadow: 2px 2px 12px rgba(0,0,0,0.1);">
    <b>Displayed Restaurants </b> {filtered_locations.filter(filtered_locations['type'] == 'restaurant').count()}<br>
    <b>Displayed Attractions </b> {filtered_locations.filter(filtered_locations['type'] == 'attraction').count()}
</div>
''', unsafe_allow_html=True)

# Show the map
folium_static(m)






