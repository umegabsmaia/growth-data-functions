import growth_data_functions.data_aquisition.get_origination_summaries_data as os
import growth_data_functions.data_aquisition.get_contracts_data as cd
import growth_data_functions.data_aquisition.get_retailers_data as rd
import growth_data_functions.data_aquisition.get_address_data as ad
import growth_data_functions.data_aquisition.get_stores_data as sd
import growth_data_functions.data_aquisition.get_application_data as apd
import pandas as pd
import numpy as np

def draft_get_originations():

    originations = os.get_origination_summaries_data()
    retailers = rd.get_retailers_data()
    contracts = cd.get_contracts_data()
    retailers = rd.get_retailers_data()
    address = ad.get_address_data()
    stores = sd.get_stores_data()
    application = apd.get_application_data() 

    df = originations

    df = pd.merge(df, contracts,how='left',on='contractId')

    df['tipo_cliente'] = np.where(df['orig_month'] == df['conv_month'], 'New', 'Recurrent')

    df = pd.merge(df,application,on='borrowerId',how='left')

    conditions_conv_type = [
        (df['orig_month'] == df['conv_month']) & (df['conv_date'] == df['app_date']),
        (df['orig_month'] == df['conv_month']) & (df['conv_date'] != df['app_date'])
    ]

    choices_conv_type = ['Same Day', 'Late Conversion']

    df['conv_type'] = np.select(conditions_conv_type, choices_conv_type, default=None)

    # Create a new column 'conv_days_since_app' based on the conditions
    df['conv_days_since_app'] = np.where(df['orig_date'] == df['conv_date'],
                                        (df['conv_date'] - df['app_date']).dt.days,
                                        None)

    conditions_product = [
        df['retailerId'].isin([168]),
        df['retailerId'].isin([157, 817]),
        df['sourceProduct'] == "HIGH_RECURRENCE",
    ]

    choices_product = ["Personal Loan", "Pix", "Ume Leve"]

    df['product'] = np.select(conditions_product, choices_product, default="Conventional")

    df.drop(columns=['sourceProduct'], inplace = True)

    df = pd.merge(df,stores,on='retailerId',how='left')

    df = pd.merge(df,retailers,on='retailerId',how='left')

    df = pd.merge(df,address, on='addressId',how='left')

    return df   

