import datetime
from dateutil import relativedelta
import pandas as pd
from sqlalchemy import create_engine, text
from IPython.core.display import display


def get_min_max_dates(date):
    """
    This function receives the today date, converts it to month format (01-mm-yyyy),
    and returns the minimum and maximum dates considering the minimum and maximum
    intervals from the get_date_by_hospital function.
    
    Inputs:
        - date (str): date in the format dd-mm-yyyy
        
    Returns:
        - min_date (str): minimum date in the format dd-mm-yyyy
        - max_date (str): maximum date in the format dd-mm-yyyy
    """
    date = datetime.datetime.strptime(date, '%d-%m-%Y')
    
    min_start_date = date + relativedelta.relativedelta(months=-2, day=16) # HGP min start date
    max_end_date = date + relativedelta.relativedelta(months=-1, day=31) # MBoi, SP Plus, and Brasilândia max end date

    return min_start_date.strftime('%Y-%m-%d'), max_end_date.strftime('%Y-%m-%d')


def get_data(date: str) -> pd.DataFrame:
    engine = create_engine('postgresql://root:123mudar@nas.cgoncalves.home:2665/datalab')
    con = engine.connect()
    min_date, max_date = get_min_max_dates(date)
    query = text("SELECT * FROM public.fechamento_plantoes WHERE \"Data\" between :min_date AND :max_date")
    df = pd.read_sql(query, con, params={"min_date": min_date, "max_date": max_date})
    return df

def get_adicionais() -> pd.DataFrame:
    """
    This function will get the data from the plantoes_adicionais table
    """
    engine = create_engine('postgresql://root:123mudar@nas.cgoncalves.home:2665/datalab')
    query = """select * FROM public.plantoes_adicionais;"""
    df = pd.read_sql(query, engine)
    return df


def get_date_by_hospital(date, hospital):
    """
    This function receives the today date, convert to month format (01-mm-yyyy)
    and a hospital string, depending on the hospital it will return the date
    return a start date and end date in the following way:
    
    - HGP: from the 16 of 2 month before to 15 of previous month
    - HRIM and HSP: from the 21 of the 2 months before to 20 of previous
    month to the last day of the previous mont
    - HMBoi, SP Plus and HMB: from the first day of previous month to the last day of that month
    
    Inputs:
        - date (str): date in the format dd-mm-yyyy
        - hospital (str): hospital name

    
    Returns:
        - start_date (str): start date in the format dd-mm-yyyy
        - end_date (str): end date in the format dd-mm-yyyy
    """
    date = datetime.datetime.strptime(date, '%d-%m-%Y')
    if hospital == 'HGP':
        start_date = date + relativedelta.relativedelta(months=-2, day=16)
        end_date = date + relativedelta.relativedelta(months=-1, day=15)
    elif hospital == 'HSP' :
        start_date = date + relativedelta.relativedelta(months=-2, day=21)
        end_date = date + relativedelta.relativedelta(months=-1, day=20)
    elif hospital == 'MBoi' or hospital == 'SP Plus'or hospital == 'Brasilândia' or hospital == 'HRIM':
        start_date = date + relativedelta.relativedelta(months=-1, day=1)
        end_date = date + relativedelta.relativedelta(months=-1, day=31)
    else:
        print('Hospital not found')
        return None
    return start_date, end_date


def filter_data(df, start_date, end_date):
    """This function will receive a dataframe 
    and a start date and end date,

    Args:
        df (pd.DataFrame): Dataframe with the plantao data
        start_date (str): start date in the format dd-mm-yyyy
        end_date (str): end date in the format dd-mm-yyyy

    Returns:
        pd.Dataframe: filtered dataframe in the range of interest
    """
    mask = (df['Data'] >= start_date) & (df['Data'] <= end_date)
    df_filtered = df.loc[mask]
    
    return df_filtered

def summary_data(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """This function will receive a dataframe 
    and a date, it will filter the data according to the date
    and then it will create a pivot table with the data

    Args:
        df (pd.DataFrame): Dataframe with the plantao data
        date (str): date in the format dd-mm-yyyy

    Returns:
        pd.DataFrame: summary of the data in a pivot table format
    """
    summaries = []
    for hospital in df['hospital'].unique():
        df_filter = df.loc[df['hospital'] == hospital]
        start_date, end_date = get_date_by_hospital(date, hospital)
        df_hospital = filter_data(df_filter, start_date, end_date)
        
        if df_hospital.empty:
            continue

        if df_hospital.loc[df_hospital['À vista'] == False].empty:
            t1 = pd.DataFrame(index=[hospital], columns=['Total Horas', 'Total a receber'])
            t1 = t1.fillna(0)
        elif df_hospital.loc[df_hospital['À vista'] == False].shape[0] == 1:
            t1 = df_hospital.loc[df_hospital['À vista'] == False, ['Total Horas', 'Valor a ser pago']].rename(columns={'Valor a ser pago': 'Total a receber'})
            t1.index = [hospital]
        else:
            t1 = (df_hospital.loc[df_hospital['À vista'] == False]
                .pivot_table(index=['hospital'], 
                            values=['Total Horas', 'Valor a ser pago'], 
                            aggfunc='sum', margins=False)
                            .rename(columns={'Valor a ser pago': 'Total a receber'})
            )

        if df_hospital.loc[df_hospital['À vista'] == True].empty:
            t2 = pd.DataFrame(index=[hospital], columns=['Horas (À vista)', 'Pago (À vista)'])
            t2 = t2.fillna(0)
        elif df_hospital.loc[df_hospital['À vista'] == True].shape[0] == 1:
            t2 = df_hospital.loc[df_hospital['À vista'] == True, ['Total Horas', 'Valor a ser pago']].rename(columns={'Valor a ser pago': 'Pago (À vista)', 'Total Horas': 'Horas (À vista)'})
            t2.index = [hospital]
        else:
            t2 = (df_hospital.loc[df_hospital['À vista'] == True]
                 .pivot_table(index=['hospital'], values=['Total Horas', 'Valor a ser pago'], 
                              aggfunc='sum', margins=False)
                    .rename(columns={'Valor a ser pago': 'Pago (À vista)', 'Total Horas': 'Horas (À vista)'})
            )

        t3 = pd.concat([t1, t2], axis=1).fillna(0)
        t3['Total Horas'] = t3['Total Horas'].astype(int)
        t3['Horas (À vista)'] = t3['Horas (À vista)'].astype(int)
        # t3['A Receber'] = t3.apply(lambda x: 0 if x['Total a receber'] - x['Pago (À vista)'] <= 0 else x['Total a receber'] - x['Pago (À vista)'], axis=1)

        summaries.append(t3)

    result = pd.concat(summaries, axis=0)

    totals = result.agg({'Total Horas': 'sum', 'Total a receber': 'sum', 'Horas (À vista)': 'sum', 'Pago (À vista)': 'sum'}).rename('Total').to_frame()
    final_result = pd.concat([result, totals.T]).fillna(' ')

    final_result = (final_result.style.format({
    'Total a receber': 'R$ {:,.2f}', 
    'Pago (À vista)': 'R$ {:,.2f}', 
    # 'A Receber': 'R$ {:,.2f}', 
    'Total Horas': '{:,.0f}h', 
    'Horas (À vista)': '{:,.0f}h'
                }))
    return final_result


def display_hospitals(df: pd.DataFrame, date: str): 
    """
    This function will receive a dataframe and a date
    and it will display the data in a table format
    Args:
        df (pd.DataFrame): Dataframe with the plantao data
        date (str): date in the format dd-mm-yyyy

    Returns:
           pd.DataFrame: detailed shift hours for each hospital     
    """  
    for hospital in df['hospital'].unique():
        df_filter = df.loc[df['hospital'] == hospital]
        start_date, end_date = get_date_by_hospital(date, hospital)
        df_filter['Observação'] = df_filter.apply(lambda row: 'Sobreaviso' if pd.isnull(row['Total Horas']) else ('À vista' if row['À vista'] else ''), axis=1)
        df_hospital = filter_data(df_filter, start_date, end_date)
        df_hospital = df_hospital.drop(df_hospital[(df_hospital['Total Horas'] == 0) & (df_hospital['Valor a ser pago'].isna())].index)
                
        # Check if the filtered DataFrame is empty and skip if it is
        if df_hospital.empty or len(df_hospital) == 0:
            continue
        
        print('-------------------------------------------------- **' + hospital + '** --------------------------------------------------')
   
        styled_table = df_hospital[['Data',
                                    'Turno', 
                                    'Total Horas', 
                                    'Valor a ser pago', 
                                    'Observação']].reset_index(drop=True).style.hide().format({
                                                                            'Valor a ser pago': 'R$ {:,.2f}'.format, 
                                                                            'Total Horas': '{:,.0f}h'.format,
                                                                            'Data': lambda x: pd.to_datetime(x).strftime('%d-%m-%Y')})
        display(styled_table)
        print('{{< pagebreak >}}')

def display_adicionais(df: pd.DataFrame, crm: str): 
    """
    This function will receive a dataframe and a date
    and it will display the data in a table format
    Args:
        df (pd.DataFrame): Dataframe with the plantao data
        date (str): date in the format dd-mm-yyyy       
    """
    df = df.query('CRM == @crm')
    if df.empty:
        return None
    if not df[df['Cargo'] == 'Coordenador'].empty:
        print('#### Adicionais')
        styled_table = df[df['Cargo'] == 'Coordenador'][['Cargo', 'Hospital', 'Valor']].reset_index(drop=True)
        display(styled_table)
    if not df[df['Cargo'].isin(['PLR', 'PLR Parcial'])].empty:
        print('#### PLR')
        styled_table = df[df['Cargo'].isin(['PLR', 'PLR Parcial'])][['Cargo', 'Valor']].rename(columns={'Cargo':'PLR'}).style.hide()
        display(styled_table)
    print('{{< pagebreak >}}')

    