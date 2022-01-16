
import json
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from datetime import date
from datetime import datetime
from datetime import timedelta
import os
import boto3
from selenium import webdriver
import pickle
import glob
import pandas
import numpy
from sqlalchemy import create_engine
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import psycopg2
import warnings
import sys
warnings.filterwarnings("ignore", category=DeprecationWarning)



time_delta = 1 # параметр для запуска скрипта. При указании N дней, скрипт запустится за дату, равную N дней назад.
td = date.today()
t_delta = timedelta(days=time_delta)


def get_s3_conn(aws_s3_bucket ="meal-analytics-dev-adjust"):

    aws_access_key = os.environ['aws_access_key']
    secret_key = os.environ['aws_secret_key']

    s3 = boto3.resource(
        service_name='s3',
        region_name='eu-central-1',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=secret_key
    )
    bucket = s3.Bucket(aws_s3_bucket)
    return bucket


def get_sql_conn():
    creds = {'usr': os.environ['db_user'],
             'pwd': os.environ['db_password'],
             'hst': 'example-of-host123.eu-central-1.rds.amazonaws.com',
             'prt': 25432,
             'dbn': 'analytics_dev'}

    connstr = 'postgresql+psycopg2://{usr}:{pwd}@{hst}:{prt}/{dbn}'

    engine = create_engine(connstr.format(**creds))
    return engine


def write_to_sql(data, base = 'qonversion_export_csv'):
    data.to_sql(base, con=get_sql_conn(), schema='public', if_exists='append', index=False)
    print('В таблицу "qonversion_export_csv" записаны данные за ', str(datetime.fromisoformat(str(td - t_delta)))[:10])

def get_data():
    global td
    global t_delta
    import time
    
    os.system("cp /opt/chrome_headless_only/chromedriver /tmp/chromedriver")
    os.system("cp /opt/chrome_headless_only/headless-chromium /tmp/headless-chromium")
    os.chmod("/tmp/chromedriver", 0o777)
    os.chmod("/tmp/headless-chromium", 0o777)
    options = Options()
    options.binary_location = '/tmp/headless-chromium'
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--single-process')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-notifications')


    options.add_experimental_option("prefs", {
      "download.default_directory": "/tmp",
      "download.prompt_for_download": False,
    })

    driver = webdriver.Chrome('/tmp/chromedriver',options=options)

    os.chdir('/tmp')
    driver.get('https://dash.qonversion.io/export/raw-data')
    
    yesterday = str(date.today() - timedelta(days=time_delta))[8:10]
    month_abbr = (date.today() - timedelta(days=time_delta)).strftime('%B')[:3]
    year = date.today().year
    
    
    #Авторизация
    driver.find_element_by_xpath('/html/body/div[1]/section/div/div/div/div/div/form/div[1]/input').send_keys(os.environ['qonv_acnt_log'])
    time.sleep(1)
    driver.find_element_by_xpath('/html/body/div[1]/section/div/div/div/div/div/form/div[2]/input').send_keys(os.environ['qonv_acnt_pass'])
    time.sleep(1)
    button = driver.find_element_by_xpath('/html/body/div[1]/section/div/div/div/div/div/form/div[4]/button')
    driver.execute_script("arguments[0].click();", button)
    time.sleep(2)
    driver.get('https://dash.qonversion.io/export/raw-data')
    # Выбор нового приложения
    time.sleep(1)
    button = driver.find_element_by_xpath('/html/body/div[1]/div[1]/div[1]/div[2]/div[1]/div/div[1]/button')
    driver.execute_script("arguments[0].click();", button)
    time.sleep(1)
    button = driver.find_element_by_xpath('/html/body/div[1]/div[1]/div[1]/div[2]/div[1]/div/div[2]/div/ul/li[3]/a')
    driver.execute_script("arguments[0].click();", button)
    time.sleep(1)
    # Выбор нужного временного диапазона
    time.sleep(1)
    button = driver.find_element_by_xpath('/html/body/div[1]/div[1]/div[2]/div[1]/div[2]/div[1]/div/div/div[2]/input')
    driver.execute_script("arguments[0].click();", button)
    time.sleep(1)
    driver.find_element_by_xpath('/html/body/div[1]/div[1]/div[2]/div[1]/div[2]/div[1]/div/div/div[2]/input').clear()
    time.sleep(1)
    button = driver.find_element_by_xpath('/html/body/div[1]/div[1]/div[2]/div[1]/div[2]/div[1]/div/div/div[2]/input')
    driver.execute_script("arguments[0].click();", button)
    time.sleep(1)
    driver.find_element_by_xpath('/html/body/div[1]/div[1]/div[2]/div[1]/div[2]/div[1]/div/div/div[2]/input').send_keys(month_abbr+' '+yesterday+',',year,'\n')
    time.sleep(1)
    button = driver.find_element_by_xpath('/html/body/div[1]/div[1]/div[2]/div[1]/div[2]/div[2]/div/div/div[2]')
    driver.execute_script("arguments[0].click();", button)
    time.sleep(2)
    # Клик по кнопке "Download"
    button = driver.find_element_by_xpath('/html/body/div[1]/div[1]/div[2]/div[1]/div[2]/div[2]/div/div/div[2]/div[2]/div/div[1]/a[2]')
    driver.execute_script("arguments[0].click();", button)
    time.sleep(5)
    driver.close()

    for file in glob.glob("/tmp/*.csv"):
        os.rename(file, 'downloaded_data.csv')

def sum_columns(data1,column1,data2,column2):
    return data1[column1].sum() - data2[column2].sum()
def count_columns(data1,column1,data2,column2):
    return data1[column1].count() - data2[column2].count()

def rd_to_sql():
    qonv_data = pandas.read_csv('downloaded_data.csv')
    data_for_sql = qonv_data.copy()
    data_for_sql.columns = data_for_sql.columns.str.replace(' ', '_', regex=False)
    data_for_sql.columns = data_for_sql.columns.str.lower()
    data_for_sql.columns = data_for_sql.columns.str.replace('local', 'local_language', regex=False)
    data_for_sql.columns = data_for_sql.columns.str.replace('country', 'country_code', regex=False)
    
    if 'advertiser_id' not in data_for_sql.columns:
        data_for_sql['advertiser_id'] = None
        
    if 'properties__q_adjust_adid' not in data_for_sql.columns:
        data_for_sql['properties__q_adjust_adid'] = None
    write_to_sql(data_for_sql)#comment this row for comparsion without writing data in base 

def have_problem_with_data():
    global td
    global t_delta
    os.listdir('/tmp')
    qonv_data = pandas.read_csv('downloaded_data.csv')
    
    
    query = '''
    select *
    from qonversion_raw qr
    where qr.time::date = '{}'
    order by qr.time
    '''.format(str(td - t_delta)[:10])
    sql_data = pandas.read_sql_query(query, get_sql_conn())

    trans_id = sum_columns(sql_data,'transaction_transaction_id',qonv_data,'Transaction ID')
    event_num = count_columns(sql_data,'event_name',qonv_data,'Event Name')
    user_id_count = count_columns(sql_data,'user_id',qonv_data,'Q User ID')
    cust_user_count = count_columns(sql_data,'custom_user_id',qonv_data,'User ID')
    device_num = count_columns(sql_data,'device_id',qonv_data,'Device ID')
    est_value = trans_id-event_num-user_id_count-cust_user_count-device_num

    if est_value == 0 :
       return False
    else:
        print('Отрицательное значение показывает, что данных больше в выгрузке из Qonversion')
        print('разница в сумме transaction_transaction_id',trans_id)
        print('разница в event_name', event_num)
        print('разница в user_id', user_id_count)
        print('разница в custom_user_id', cust_user_count)
        print('разница в device_id', device_num)
        return True


def slack_failed_task(context):
    slack_token = os.environ['slack_bot_token']
    client = WebClient(token=slack_token)
    try:
        response = client.chat_postMessage(
            channel=os.environ['slack_channel'],
            text=context
        )

    except SlackApiError as e:
        assert e.response["error"]

def start_testing():
    global td
    global t_delta
    get_data()
    if (have_problem_with_data()):
        slack_failed_task("Есть расхождения между данными в базе и в отчете из Qonversion за " + str(td - t_delta)[:10])
    else:
        print('Данные за', str(td - t_delta)[:10],' совпадают')


def lambda_handler(event, context):

    try:
        start_testing()
    except:
        slack_failed_task("Проверка данных Qonversion за  " + str(td - t_delta)[:10]+' не состоялась из-за непредвиденной ошибки. Лямбда-функция "SeleniumTest"')
    try:
        rd_to_sql()
    except:
        slack_failed_task("Проблема с записью raw_data из веб-интерфейса Qonversion за  " + str(td - t_delta)[:10]+'. Лямбда-функция "SeleniumTest"')
    return True
