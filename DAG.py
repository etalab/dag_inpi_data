from this import d
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from operators.clean_folder import CleanFolderOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import os
from minio import Minio
import glob
import unidecode
import pandas as pd
import sqlite3
import requests
import json

MINIO_URL = Variable.get("MINIO_URL")
MINIO_BUCKET = Variable.get("MINIO_BUCKET_OPENDATA")
MINIO_USER = Variable.get("SECRET_MINIO_USER_OPENDATA")
MINIO_PASSWORD = Variable.get("SECRET_MINIO_PASSWORD_OPENDATA")

INPI_USER = Variable.get("SECRET_INPI_USER")
INPI_PWD = Variable.get("SECRET_INPI_PASSWORD")

client = Minio(
    MINIO_URL,
    access_key=MINIO_USER,
    secret_key=MINIO_PASSWORD,
    secure=True,
)

TMP_FOLDER = '/tmp/inpi/'
PATH_MINIO_INPI_DATA = 'inpi/'
PATH_MINIO_PROCESSED_INPI_DATA = 'ae/'

yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

def upload_minio_synthese_files():
    # check if bucket exists.
    found = client.bucket_exists(MINIO_BUCKET)
    if found:
        for path, subdirs, files in os.walk(TMP_FOLDER+"synthese/"):
            for name in files:
                print(os.path.join(path, name))
                isFile = os.path.isfile(os.path.join(path, name))
                if isFile:
                    client.fput_object(
                        MINIO_BUCKET,
                        "/"+PATH_MINIO_INPI_DATA
                        + os.path.join(path, name).replace(
                            TMP_FOLDER, ""
                        ),
                        os.path.join(path, name),
                    )

def upload_minio_original_files():
    # check if bucket exists.
    found = client.bucket_exists(MINIO_BUCKET)
    if found:
        for path, subdirs, files in os.walk(TMP_FOLDER+"flux-tc/"):
            for name in files:
                print(os.path.join(path, name))
                isFile = os.path.isfile(os.path.join(path, name))
                if isFile:
                    client.fput_object(
                        MINIO_BUCKET,
                        "/"+PATH_MINIO_INPI_DATA
                        + os.path.join(path, name).replace(
                            TMP_FOLDER, ""
                        ),
                        os.path.join(path, name),
                    )
        for path, subdirs, files in os.walk(TMP_FOLDER+"stock/"):
            for name in files:
                print(os.path.join(path, name))
                isFile = os.path.isfile(os.path.join(path, name))
                if isFile:
                    client.fput_object(
                        MINIO_BUCKET,
                        "/"+PATH_MINIO_INPI_DATA
                        + os.path.join(path, name).replace(
                            TMP_FOLDER, ""
                        ),
                        os.path.join(path, name),
                    )

# Etape de processing des données en amont du lancement de l'enrichissement de la bdd sqlite
def concatFilesRep(days, type_file, name_concat, pattern): #"flux-tc", "_flux_rep", "*/*/*_5_rep.csv"
    for d in days:
        pathdate = TMP_FOLDER + type_file + "/" + d + "/"
        consofile = TMP_FOLDER + 'synthese/' + '-'.join(d.split('/')) + name_concat + '.csv' #?
        # get list files stock
        list_files = glob.glob(pathdate + pattern)
        list_files.sort()
        if len(list_files) > 0:
            with open(consofile,"wb") as fout:
                # first file:
                with open(list_files[0], "rb") as f:
                    fout.write(f.read())
                # now the rest:    
                for ls in list_files[1:]:
                    with open(ls, "rb") as f:
                        try:
                            next(f) # skip the header
                        except:
                            pass
                        fout.write(f.read())
        
        
        #os.system('rm ' + consofile + ' && ulimit -n 10240 && head -n1 ' + list_files[0] + ' > ' + consofile)
        #for ls in list_files:
        #    os.system('tail -n+2 -q ' + ls + ' >> ' + consofile)
        
        print(name_concat + " ok")

def get_latest_db(ti):
    client.fget_object("opendata", PATH_MINIO_INPI_DATA+"inpi.db", TMP_FOLDER+"inpi.db")
    
    start_date=ti.xcom_pull(key='start_date', task_ids='get_start_date')
    start = datetime.strptime(start_date,'%Y-%m-%d')
    end = datetime.today()-timedelta(days=1)
    delta = end - start  # as timedelta
    days = [datetime.strftime(start + timedelta(days=i),'%Y/%m/%d') for i in range(delta.days + 1)]

    concatFilesRep(days, "stock", "_stock_rep", "*/*/*_5_rep.csv")
    concatFilesRep(days, "flux-tc", "_flux_rep", "*/*/*_5_rep.csv")
    concatFilesRep(days, "flux-tc", "_flux_rep_nouveau_modifie", "*/*/*6_rep_nouveau_modifie_EVT.csv")
    concatFilesRep(days, "flux-tc", "_flux_rep_partant", "*/*/*7_rep_partant_EVT.csv")

def uniformizeDf(df):
    for c in df.columns:
        df = df.rename(columns={c: unidecode.unidecode(
                                c.lower() \
                                .replace(' ', '') \
                                .replace('.','') \
                                .replace('_','') \
                                .replace('"','') \
                                .replace("'",'')
                    )})
                    
    if 'sigle' not in df.columns:
        df['sigle'] = ''
    # Do something
    df['siren'] = df.fillna('')['siren']
    df['nom_patronymique'] = df.fillna('')['nompatronymique']
    df['nom_usage'] = df.fillna('')['nomusage']
    df['prenoms'] = df.fillna('')['prenoms']
    df['nom_patronymique'] = df['nom_patronymique'].apply(lambda x: str(x).replace(',','').lower())
    df['nom_usage'] = df['nom_usage'].apply(lambda x: str(x).replace(',','').lower())
    df['prenoms'] = df['prenoms'].apply(lambda x: str(x).replace(',','').lower())
    df['datenaissance'] = df.fillna('')['datenaissance']
    df['villenaissance'] = df.fillna('')['villenaissance']
    df['paysnaissance'] = df.fillna('')['paysnaissance']
    df['denomination'] = df.fillna('')['denomination']
    df['siren_pm'] = df.fillna('')['siren1']
    df['sigle'] = df.fillna('')['sigle']
    df['qualite'] = df.fillna('')['qualite']
    dfpp = df[df['type'].str.contains('Physique')][['siren', 'nom_patronymique', 'nom_usage','prenoms', 'datenaissance', 'villenaissance', 'paysnaissance', 'qualite']]
    dfpm = df[df['type'].str.contains('Morale')][['siren','denomination', 'siren_pm', 'sigle', 'qualite']]

    return dfpp, dfpm


# Toutes les dates après la date du stock initial
def update_db(ti, **kwargs):

    start_date=ti.xcom_pull(key='start_date', task_ids='get_start_date')
    start = datetime.strptime(start_date,'%Y-%m-%d')
    end = datetime.today()-timedelta(days=1)
    delta = end - start  # as timedelta
    days = [datetime.strftime(start + timedelta(days=i),'%Y-%m-%d') for i in range(delta.days + 1)]

    templates_dict = kwargs.get("templates_dict")
    connection = sqlite3.connect(TMP_FOLDER+"inpi.db")
    cursor = connection.cursor()

    for d in days:

        consofile = TMP_FOLDER + 'synthese/' + d + '_stock_rep.csv'
        # Manage stocks
        if(os.path.exists(consofile)):
            df = pd.read_csv(consofile,sep=";",dtype=str,warn_bad_lines='skip')
            dfpp, dfpm = uniformizeDf(df)
            print('loaded and uniformize')
            # Add new stock
            dfpp.set_index('siren').to_sql('rep_pp', con=connection, if_exists='append')
            dfpm.set_index('siren').to_sql('rep_pm', con=connection, if_exists='append')
            print('Stock processed : ' + str(dfpp.shape[0]+dfpm.shape[0]) + 'added records')

        
        consofile = TMP_FOLDER + 'synthese/' + d + '_flux_rep.csv'
        if(os.path.exists(consofile)):
            df = pd.read_csv(consofile,sep=";",dtype=str,warn_bad_lines=True)
            dfpp, dfpm = uniformizeDf(df)
            dfpp.set_index('siren').to_sql('rep_pp', con=connection, if_exists='append')
            dfpm.set_index('siren').to_sql('rep_pm', con=connection, if_exists='append')
            print('Flux rep processed : ' + str(dfpp.shape[0]+dfpm.shape[0]) + 'added records')

        consofile = TMP_FOLDER + 'synthese/' + d + '_flux_rep_nouveau_modifie.csv'
        if(os.path.exists(consofile)):
            df = pd.read_csv(consofile,sep=";",dtype=str,warn_bad_lines=True)
            dfpp, dfpm = uniformizeDf(df)
            dfpp.set_index('siren').to_sql('rep_pp', con=connection, if_exists='append')
            dfpm.set_index('siren').to_sql('rep_pm', con=connection, if_exists='append')
            print('Flux rep modified new processed : ' + str(dfpp.shape[0]+dfpm.shape[0]) + 'added records')


        consofile = TMP_FOLDER + 'synthese/' + d + '_flux_rep_partant.csv'
        if(os.path.exists(consofile)):
            df = pd.read_csv(consofile,sep=";",dtype=str,warn_bad_lines=True)
            dfpp, dfpm = uniformizeDf(df)
            # Delete each rep
            result = 0
            for index,row in dfpp.iterrows():
                del_query = """DELETE from rep_pp where siren = ? AND nom_patronymique = ? AND prenoms = ? AND qualite = ?"""
                cursor.execute(del_query, (row['siren'], row['nom_patronymique'], row['prenoms'], row['qualite']))
                result = result + cursor.rowcount
            connection.commit()
            for index,row in dfpm.iterrows():
                del_query = """DELETE from rep_pm where siren = ? AND siren_pm = ? AND qualite = ?"""
                cursor.execute(del_query, (row['siren'], row['siren_pm'], row['qualite']))
                result = result + cursor.rowcount
            connection.commit()
            print('Flux rep partant processed : ' + str(result) + ' deleted records')

def upload_minio_enriched_files():
    #client.fput_object("opendata", PATH_MINIO_PROCESSED_INPI_DATA+"rep_pm.csv", TMP_FOLDER+"rep_pm.csv",)
    #client.fput_object("opendata", PATH_MINIO_PROCESSED_INPI_DATA+"rep_pp.csv", TMP_FOLDER+"rep_pp.csv",)
    client.fput_object("opendata", PATH_MINIO_INPI_DATA+"inpi.db", TMP_FOLDER+"inpi.db",)

def check_emptiness():
    if len(glob.glob(TMP_FOLDER+'flux-tc/*')) != 0:
        return True
    else:
        if len(glob.glob(TMP_FOLDER+'stock/*')) != 0:
            return True
        else:
            return False

def get_start_date_minio(ti):
    r = requests.get('https://object.files.data.gouv.fr/opendata/ae/latest_inpi_date.json')
    start_date = r.json()['latest_date']
    dt_sd = datetime.strptime(start_date, '%Y-%m-%d')
    start_date = datetime.strftime((dt_sd + timedelta(days=1)), '%Y-%m-%d')
    ti.xcom_push(key='start_date', value=start_date) 

def get_latest_files_from_start_date(ti):
    start_date=ti.xcom_pull(key='start_date', task_ids='get_start_date')
    start = datetime.strptime(start_date,'%Y-%m-%d')
    end = datetime.today()-timedelta(days=1)
    delta = end - start  # as timedelta
    days = [datetime.strftime(start + timedelta(days=i),'%Y-%m-%d') for i in range(delta.days + 1)]
    for day in days:
        print('Retrieving inpi files from {}'.format(day))
        get_latest_files_bash = BashOperator(
            task_id='get_latest_files_bash',
            bash_command='/opt/airflow/dags/dag_inpi_data/get.sh '+day+' '+INPI_USER+' '+INPI_PWD,
        )
        get_latest_files_bash.execute(dict())

def upload_latest_date_inpi_minio():
    latest_date = datetime.strftime((datetime.today()-timedelta(days=1)),'%Y-%m-%d')
    data = {}
    data['latest_date'] = latest_date
    with open(TMP_FOLDER+"latest_inpi_date.json", "w") as write_file:
        json.dump(data, write_file)    

    client.fput_object(
        bucket_name="opendata",
        object_name=PATH_MINIO_PROCESSED_INPI_DATA+"latest_inpi_date.json",
        file_path=TMP_FOLDER+"latest_inpi_date.json",
        content_type="application/json"
    )

with DAG(
    dag_id='inpi-dirigeants',
    schedule_interval='0 14 * * *',
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=180),
    tags=['inpi', 'dirigeants'],
    params={},
) as dag:

    clean_previous_outputs = CleanFolderOperator(
        task_id="clean_previous_outputs",
        folder_path=TMP_FOLDER
    )

    get_start_date = PythonOperator(
        task_id="get_start_date", 
        python_callable=get_start_date_minio
    )

    get_latest_files = PythonOperator(
        task_id="get_latest_files", 
        python_callable=get_latest_files_from_start_date
    )

    is_empty_folders = ShortCircuitOperator(
        task_id="is_empty_folders", 
        python_callable=check_emptiness
    )

    upload_inpi_files_to_minio = PythonOperator(
        task_id="upload_inpi_files_to_minio", 
        python_callable=upload_minio_original_files
    )

    get_latest_sqlite_db = PythonOperator(
        task_id="get_latest_sqlite_db",
        python_callable=get_latest_db
    )

    update_sqlite_db = PythonOperator(
        task_id="update_sqlite_db",
        python_callable=update_db
    )

    upload_result_files_to_minio = PythonOperator(
        task_id="upload_result_files_to_minio",
        python_callable=upload_minio_enriched_files
    )

    upload_synthese_files_to_minio = PythonOperator(
        task_id="upload_synthese_files_to_minio",
        python_callable=upload_minio_synthese_files
    )

    upload_latest_date_inpi = PythonOperator(
        task_id="upload_latest_date_inpi",
        python_callable=upload_latest_date_inpi_minio
    )

    get_start_date.set_upstream(clean_previous_outputs)
    get_latest_files.set_upstream(get_start_date)
    is_empty_folders.set_upstream(get_latest_files)
    upload_inpi_files_to_minio.set_upstream(is_empty_folders)
    get_latest_sqlite_db.set_upstream(is_empty_folders)
    update_sqlite_db.set_upstream(get_latest_sqlite_db)
    upload_result_files_to_minio.set_upstream(update_sqlite_db)
    upload_synthese_files_to_minio.set_upstream(upload_result_files_to_minio)

    upload_latest_date_inpi.set_upstream(upload_synthese_files_to_minio)
    upload_latest_date_inpi.set_upstream(upload_inpi_files_to_minio)
