#### Dataset Preparation

# Imports
import pandas as pd
import requests
import io

# Datasets URL
dataset_urls = {
    'criminal_acts': 'https://analisi.transparenciacatalunya.cat/resource/y48r-ae59.json?$limit=50000000'
}

# Retrieving and Reading Criminal Dataset
criminal_dataset = requests.get(dataset_urls['criminal_acts']).content
criminal_dataset = pd.read_json(io.StringIO(criminal_dataset.decode('utf-8')))

def preprocessing_criminal_dataset(dataset, list_not_domain=spaces_not_in_domain):
    """
    Preprocesses the criminal dataset.
    
    Args:
        :param dataset: The original criminal dataset.
        :param list_not_domain: List of elements not relevant to the tourism domain.
    
    Returns:
        Preprocessed criminal dataset.
    """
    dataset = dataset.rename(columns={'n_m_mes': 'num_mes',
                                       'regi_policial_rp': 'regio_policial',
                                       'rea_b_sica_policial_abp': 'area_basica_policial',
                                       'prov_ncia': 'provincia',
                                       't_tol_del_fet_codi_penal': 'titol_del_fet_codi_penal',
                                       'tipus_de_fet_codi_penal_o': 'tipus_de_fet_codi_penal',
                                       'mbit_fet': 'ambit_fet',
                                       'nombre_v_ctimes': 'nombre_victimes'})
    
    dataset = dataset.loc[dataset["provincia"] == 'Barcelona']
    
    mask = dataset['tipus_de_lloc_dels_fets'].apply(lambda x: any(space in str(x) for space in list_not_domain))
    dataset = dataset[~mask]

    return dataset

# Elements not in domain
spaces_not_in_domain = ["Empr. tèxtil, calçat i complementaris", "Taller de Vehicles_CC", "Obra en construcció", "Sindicat", "Centre de Menors",
                        "Partit polític", "Empresa de la construcció", "Centre d'Atenció Primària", "Associació de veïns", "Institut d'Educació",
                        "Empreses de productes d'alimentació/begudes", "Dependències",  "Escola",  "Oficines/Despatxos professionals",
                        "Centre de dia/Residència persones grans"]

criminal_dataset = preprocessing_criminal_dataset(criminal_dataset)



