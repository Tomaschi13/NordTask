from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import pandas_gbq
import ast
from google.oauth2.service_account import Credentials


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'trends_dag',
    default_args=default_args,
    description='Extract, transform, and load Google Trends data',
    schedule_interval='@weekly'
)

def extract_trends(date_start=None, date_end=None):

    if not date_end:
        date_end = datetime.today() - timedelta(days=1)
        date_end_str = date_end.strftime('%Y-%m-%d')
    else:
        date_end_str = date_end
    if not date_start:
        date_start = date_end - timedelta(days=7)
        date_start_str = date_start.strftime('%Y-%m-%d')
    else:
        date_start_str = date_start

    date_formats = ['%m/%d/%y', '%m/%d/%Y', '%Y-%m-%d', '%d-%b-%y']
    for fmt in date_formats:
        try:
            date_start = datetime.strptime(date_start_str, fmt)
            date_end = datetime.strptime(date_end_str, fmt)
            break
        except ValueError:
            pass
    else:
        return 'Invalid date format. Please use mm/dd/yy, mm/dd/yyyy, yyyy-mm-dd, or dd-Mon-yy format.'

    if date_start < datetime(2004, 1, 1) or date_end < datetime(2004, 1, 1):
        return 'Start date cannot be before 2004-01-01.'
    if date_end > datetime.today() or date_start > datetime.today():
        return 'Date cannot be in the future.'

    if date_end < date_start:
        return 'End date cannot be before start date.'
    
    date_start = date_start.strftime('%Y-%m-%d')
    date_end = date_end.strftime('%Y-%m-%d')

    ccs = ['AI', 'FM', 'FK', 'CK', 'ET', 'AS', 'SM', 'MP', 'DM', 'MF', 'VU', 'SB', 'YT', 'GQ', 'LI', 'FO', 'WS', 'CF', 'IR', 'SC', 'GG', 'SX', 'GI', 'BT', 'BZ', 'PH', 'CN', 'TC', 'VI', 'MM', 'TM', 'BI', 'TG', 'CW', 'DJ', 'SL', 'SS', 'KY', 'SH', 'SZ', 'GL', 'SY', 'GU', 'JM', 'VN', 'PG', 'IM', 'LS', 'BW', 'LR', 'US', 'EH', 'NA', 'ZA', 'JE', 'BS', 'AG', 'AW', 'SO', 'SN', 'GA', 'GF', 'KE', 'ZW', 'AF', 'FJ', 'TL', 'TT', 'GD', 'CG', 'VC', 'BL', 'GY', 'MT', 'NP', 'KH', 'GM', 'NC', 'ID', 'MO', 'AD', 'AU', 'MW', 'VE', 'SG', 'BB', 'PK', 'GH', 'MV', 'CI', 'MA', 'CU', 'BJ', 'MR', 'BN', 'BD', 'NL', 'NZ', 'QA', 'BM', 'CA', 'ZM', 'AE', 'MY', 'UG', 'TZ', 'BO', 'NG', 'HK', 'SD', 'ML', 'IE', 'XK', 'SR', 'AT', 'PR', 'GB', 'MU', 'YE', 'LK', 'IS', 'AL', 'BE', 'CZ', 'TD', 'PY', 'OM', 'HT', 'TN', 'AR', 'RW', 'SK', 'BH', 'MG', 'CM', 'EC', 'TW', 'JP', 'IN', 'HU', 'NO', 'MN', 'NE', 'LB', 'CV', 'LC', 'FR', 'GN', 'ES', 'PA', 'CL', 'SE', 'IT', 'MQ', 'CR', 'PL', 'CO', 'RU', 'IQ', 'PT', 'BR', 'MX', 'KR', 'PE', 'DK', 'UY', 'RE', 'BF', 'GT', 'SI', 'HN', 'AZ', 'RO', 'DO', 'EE', 'NI', 'TH', 'GE', 'TR', 'GR', 'LT', 'LY', 'SV', 'GP', 'SA', 'CY', 'FI', 'BG', 'DZ', 'UZ', 'CD', 'KW', 'LU', 'JO', 'MZ', 'LA', 'BA', 'HR', 'AM', 'IL', 'LV', 'MK', 'RS', 'CH', 'UA', 'EG', 'MD', 'AO', 'TJ', 'DE', 'PS', 'BY', 'ME', 'KG', 'KZ', 'NR', 'PM', 'AX', 'AQ', 'TF', 'BQ', 'BV', 'CC', 'KM', 'CX', 'GW', 'HM', 'IO', 'KI', 'KN', 'MC', 'MH', 'MS', 'NF', 'NU', 'PN', 'PW', 'KP', 'PF', 'GS', 'SJ', 'ST', 'TK', 'TO', 'TV', 'UM', 'VA', 'VG', 'WF']

    options = webdriver.ChromeOptions()
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36')
    options.add_experimental_option('detach', True)
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.binary_location = '/usr/bin/google-chrome-stable'
    path = '/chromedriver_linux64/chromedriver'
    driver = webdriver.Chrome(path, options=options)

    driver.get('https://trends.google.com/trends/explore?date=now%207-d&geo=RU&q=vpn,hack,cyber,security,wifi')
    time.sleep(5)
    driver.refresh()
    time.sleep(5)

    search_shares = []
    for cc in ccs:
        url = 'https://trends.google.com/trends/explore?date=' + date_start + '%20' + date_end + '&geo=' + cc + '&q=vpn,hack,cyber,security,wifi'
        driver.get(url)    
        time.sleep(5)
        elements = driver.find_elements(By.CSS_SELECTOR, '.bar-chart-content div[aria-label="A tabular representation of the data in the chart."]:first-of-type table > tbody > tr td:not(:first-child)')
        info = driver.find_element(By.CLASS_NAME, 'sticky-legend-filters-description').get_property('textContent')
        infos = info.split(',')

        term_share = []
        if len(elements) > 0:
            for element in elements:
                term_share.append(int(element.get_property('textContent')))
        else: 
            term_share += [0] * 5

        
        new_data = {
            'term_share': term_share,
            'country': infos[0].strip(),
            'date_start': date_start,
            'date_end': date_end
        }
        search_shares.append(new_data)

    driver.quit()
    return search_shares

def transform_trends(countries):
    countries_list = ast.literal_eval(countries)
    df = pd.DataFrame(columns=['country', 'search_term', 'rank', 'share', 'date_start', 'date_end', 'upload_date'])
    
    for value in countries_list:
        data = {'vpn': value['term_share'][0], 
                'hack': value['term_share'][1], 
                'cyber': value['term_share'][2], 
                'security': value['term_share'][3], 
                'wifi': value['term_share'][4]}
        

        sorted_data = dict(sorted(data.items(), key=lambda x: (-x[1], x[0] == 'vpn', x[0])))
        i = 0
        current_date = datetime.now().date()
        formatted_date = current_date.strftime('%Y-%m-%d')

        for key, v in sorted_data.items():
            i = i + 1
            df = df.append({'country': value['country'], 
                            'search_term' : key, 
                            'rank' : i, 
                            'share' : v, 
                            'date_start' : value['date_start'], 
                            'date_end' : value['date_end'],
                            'upload_date': formatted_date},
                            ignore_index=True)

    return df.to_dict(orient='records')

def upload_trends(**context):    
    df = context['ti'].xcom_pull(task_ids='transform_trends')
    df = pd.DataFrame(df)

    schema = [
        {'name': col, 'type': str(df[col].dtype)} for col in df.columns
    ]

    required_field = {'name': 'country', 'type': 'string', 'mode': 'REQUIRED'}
    schema.append(required_field)

    project_id = 'homework-data2020'
    dataset_id ='data_engineer'
    table_id = 'tt_search_term_rankings'

    key_path = '/path'
    credentials = Credentials.from_service_account_file(key_path)

    pandas_gbq.to_gbq(df, f'{dataset_id}.{table_id}', project_id=project_id, credentials=credentials, if_exists='append')

    return 'Data was uploaded'


extract_task = PythonOperator(
    task_id='extract_trends',
    python_callable=extract_trends,
    op_kwargs={'date_start': '2022-03-05', 'date_end': '2022-03-09'},
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_trends',
    python_callable=transform_trends,
    op_kwargs={'countries': '{{ ti.xcom_pull(task_ids="extract_trends") }}'},
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_trends',
    python_callable=upload_trends,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> upload_task
