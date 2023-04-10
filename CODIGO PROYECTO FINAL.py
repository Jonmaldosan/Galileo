# %%
import pandas as pd
import numpy as np
from faker import Faker
import random
import datetime
import boto3
import psycopg2
import configparser
import matplotlib.pyplot as plt

# %%
config = configparser.ConfigParser()
config.read('config.cfg')

# %% [markdown]
# ### CREACION DE INSTANCIAS EN RDS

# %%
aws_conn = boto3.client('rds', aws_access_key_id=config.get('IAM', 'ACCESS_KEY'),
                    aws_secret_access_key=config.get('IAM', 'SECRET_ACCESS_KEY'),
                    region_name='us-east-1')

# %% [markdown]
# ### VERIFICACION DE INSTANCIAS EN RDS

# %%
rdsInstanceIds = []

response = aws_conn.describe_db_instances()
for resp in response['DBInstances']:
    rdsInstanceIds.append(resp['DBInstanceIdentifier'])
    db_instance_status = resp['DBInstanceStatus']

print(f"DBInstanceIds {rdsInstanceIds}")

# %%
rds1Identifier = 'OLTP'

# %% [markdown]
# ### CREACION DE LOS SERVICIOS EN RDS PARA EL OLTP

# %%
try:
    response = aws_conn.create_db_instance(
            AllocatedStorage=10,
            DBName=config.get('OLTP', 'DB_NAME'),
            DBInstanceIdentifier=rds1Identifier,
            DBInstanceClass="db.t3.micro",
            Engine="postgres",
            MasterUsername=config.get('OLTP', 'DB_USER'),
            MasterUserPassword=config.get('OLTP', 'DB_PASSWORD'),
            Port=int(config.get('OLTP', 'DB_PORT')),
            VpcSecurityGroupIds=[config.get('VPC', 'SECURITY_GROUP')],
            PubliclyAccessible=True
        )
    print(response)
except aws_conn.exceptions.DBInstanceAlreadyExistsFault as ex:
    print("La Instancia de Base de Datos ya Existe.")

# %% [markdown]
# ### VALIDACION DE CONEXION

# %%
try:
     instances = aws_conn.describe_db_instances(DBInstanceIdentifier=rds1Identifier)
     RDS_HOST = instances.get('DBInstances')[0].get('Endpoint').get('Address')
     print(RDS_HOST)
except Exception as ex:
     print("La instancia de base de datos no existe o aun no se ha terminado de crear.")
     print(ex)

# %% [markdown]
# ### CREACION DE TABLAS REGISTRO DE TRANSACCIONES

# %%
import SQL_OLTP

try:
    db_conn = psycopg2.connect(
        database=config.get('OLTP', 'DB_NAME'), 
        user=config.get('OLTP', 'DB_USER'),
        password=config.get('OLTP', 'DB_PASSWORD'), 
        host=RDS_HOST,
        port=config.get('OLTP', 'DB_PORT')
    )

    cursor = db_conn.cursor()
    cursor.execute(SQL_OLTP.DDL_QUERY)
    db_conn.commit()
    print("Base de Datos Creada Exitosamente")
except Exception as ex:
    print("ERROR: Error al crear la base de datos.")
    print(ex)

# %%
def insertDataToSQL(data_dict, table_name):
     postgres_driver = f"""postgresql://{config.get('OLTP', 'DB_USER')}:{config.get('OLTP', 'DB_PASSWORD')}@{RDS_HOST}:{config.get('OLTP', 'DB_PORT')}/{config.get('OLTP', 'DB_NAME')}"""    
     df_data = pd.DataFrame.from_records(data_dict)
     try:
          response = df_data.to_sql(table_name, postgres_driver, index=False, if_exists='append')
          print(f'Se han insertado {response} nuevos registros.' )
     except Exception as ex:
          print(ex)

# %% [markdown]
# ### CONEXION HACIA AWS S3

# %%
s3 = boto3.resource(
    service_name = 's3',
    region_name = 'us-east-1',
    aws_access_key_id = config.get('IAM', 'ACCESS_KEY'),
    aws_secret_access_key = config.get('IAM', 'SECRET_ACCESS_KEY')
)

# %%
for bucket in s3.buckets.all():
    S3_BUCKET_NAME = bucket.name
    print(bucket.name)

# %%
S3_BUCKET_NAME = 'dspython-2023'

# %%
remoteFileList = []
for objt in s3.Bucket(S3_BUCKET_NAME).objects.all():
    remoteFileList.append(objt.key)

remoteFileList

# %% [markdown]
# ### CARGA DE DATOS OLTP

# %%
import io

for remoteFile in remoteFileList:
    try:
        file = s3.Bucket(S3_BUCKET_NAME).Object(remoteFile).get()
        data = file['Body'].read()
        df_categoria = pd.read_csv('CATEGORIA.csv')
    except Exception as ex:
        print("No es un archivo.")
        print(ex)

df_categoria.head()

# %%
insertDataToSQL(df_categoria, 'categoria')

# %%
import io

for remoteFile in remoteFileList:
    try:
        file = s3.Bucket(S3_BUCKET_NAME).Object(remoteFile).get()
        data = file['Body'].read()
        df_region = pd.read_csv('REGION.csv')
    except Exception as ex:
        print("No es un archivo.")
        print(ex)

df_region.head()

# %%
insertDataToSQL(df_region, 'region')

# %%
import io

for remoteFile in remoteFileList:
    try:
        file = s3.Bucket(S3_BUCKET_NAME).Object(remoteFile).get()
        data = file['Body'].read()
        df_departamento = pd.read_csv('DEPARTAMENTO.csv')
    except Exception as ex:
        print("No es un archivo.")
        print(ex)

df_departamento.head()

# %%
insertDataToSQL(df_departamento, 'departamento')

# %%
import io

for remoteFile in remoteFileList:
    try:
        file = s3.Bucket(S3_BUCKET_NAME).Object(remoteFile).get()
        data = file['Body'].read()
        df_municipio = pd.read_csv('MUNICIPIO.csv')
    except Exception as ex:
        print("No es un archivo.")
        print(ex)

df_municipio.head()

# %%
insertDataToSQL(df_municipio, 'municipio')

# %%
import io

for remoteFile in remoteFileList:
    try:
        file = s3.Bucket(S3_BUCKET_NAME).Object(remoteFile).get()
        data = file['Body'].read()
        df_subcategoria = pd.read_csv('SUBCATEGORIA.csv')
    except Exception as ex:
        print("No es un archivo.")
        print(ex)

df_subcategoria.head()

# %%
insertDataToSQL(df_subcategoria, 'subcategoria')

# %%
import io

for remoteFile in remoteFileList:
    try:
        file = s3.Bucket(S3_BUCKET_NAME).Object(remoteFile).get()
        data = file['Body'].read()
        df_producto = pd.read_csv('PRODUCTO.csv')
    except Exception as ex:
        print("No es un archivo.")
        print(ex)

df_producto.head()

# %%
insertDataToSQL(df_producto, 'producto')

# %%
import io

for remoteFile in remoteFileList:
    try:
        file = s3.Bucket(S3_BUCKET_NAME).Object(remoteFile).get()
        data = file['Body'].read()
        df_segmento = pd.read_csv('SEGMENTO.csv')
    except Exception as ex:
        print("No es un archivo.")
        print(ex)

df_segmento.head()

# %%
insertDataToSQL(df_segmento, 'segmento')

# %%
import io

for remoteFile in remoteFileList:
    try:
        file = s3.Bucket(S3_BUCKET_NAME).Object(remoteFile).get()
        data = file['Body'].read()
        df_cliente = pd.read_csv('CLIENTE.csv')
    except Exception as ex:
        print("No es un archivo.")
        print(ex)

df_cliente.head()

# %%
insertDataToSQL(df_cliente, 'cliente')

# %%
import io

for remoteFile in remoteFileList:
    try:
        file = s3.Bucket(S3_BUCKET_NAME).Object(remoteFile).get()
        data = file['Body'].read()
        df_gerente = pd.read_csv('GERENTE.csv')
    except Exception as ex:
        print("No es un archivo.")
        print(ex)

df_gerente.head()

# %%
insertDataToSQL(df_gerente, 'gerente')

# %%
import io

for remoteFile in remoteFileList:
    try:
        file = s3.Bucket(S3_BUCKET_NAME).Object(remoteFile).get()
        data = file['Body'].read()
        df_store = pd.read_csv('STORE.csv')
    except Exception as ex:
        print("No es un archivo.")
        print(ex)

df_store.head()

# %%
insertDataToSQL(df_store, 'store')

# %%
import io

for remoteFile in remoteFileList:
    try:
        file = s3.Bucket(S3_BUCKET_NAME).Object(remoteFile).get()
        data = file['Body'].read()
        df_envio = pd.read_csv('ENVIO.csv')
    except Exception as ex:
        print("No es un archivo.")
        print(ex)

df_envio.head()

# %%
insertDataToSQL(df_envio, 'envio')

# %%
import io

for remoteFile in remoteFileList:
    try:
        file = s3.Bucket(S3_BUCKET_NAME).Object(remoteFile).get()
        data = file['Body'].read()
        df_ventas = pd.read_csv('VENTAS.csv')
    except Exception as ex:
        print("No es un archivo.")
        print(ex)

df_ventas.head()

# %%
insertDataToSQL(df_ventas, 'ventas')

# %%
import io

for remoteFile in remoteFileList:
    try:
        file = s3.Bucket(S3_BUCKET_NAME).Object(remoteFile).get()
        data = file['Body'].read()
        df_devolucion = pd.read_csv('DEVOLUCIONES.csv')
    except Exception as ex:
        print("No es un archivo.")
        print(ex)

df_devolucion.head()

# %%
insertDataToSQL(df_devolucion, 'devolucion')

# %% [markdown]
# ### CREACION DE LOS RECURSOS EN RDS PARA DATA WAREHOUSE

# %%
rds2Identifier = 'DWH'

# %%
try:
    response = aws_conn.create_db_instance(
            AllocatedStorage=10,
            DBName=config.get('DWH', 'DB_NAME'),
            DBInstanceIdentifier=rds2Identifier,
            DBInstanceClass="db.t3.micro",
            Engine="postgres",
            MasterUsername=config.get('DWH', 'DB_USER'),
            MasterUserPassword=config.get('DWH', 'DB_PASSWORD'),
            Port=int(config.get('DWH', 'DB_PORT')),
            VpcSecurityGroupIds=[config.get('VPC', 'SECURITY_GROUP')],
            PubliclyAccessible=True
        )
    print(response)
except aws_conn.exceptions.DBInstanceAlreadyExistsFault as ex:
    print("La Instancia de Base de Datos ya Existe.")

# %%
try:
     instances = aws_conn.describe_db_instances(DBInstanceIdentifier=rds2Identifier)
     DWH_HOST = instances.get('DBInstances')[0].get('Endpoint').get('Address')
     print(DWH_HOST)
except Exception as ex:
     print("La instancia de base de datos no existe o aun no se ha terminado de crear.")
     print(ex)

# %%
import SQL_DWH

try:
    db_conn = psycopg2.connect(
        database=config.get('DWH', 'DB_NAME'), 
        user=config.get('DWH', 'DB_USER'),
        password=config.get('DWH', 'DB_PASSWORD'), 
        host=DWH_HOST,
        port=config.get('DWH', 'DB_PORT')
    )

    cursor = db_conn.cursor()
    cursor.execute(SQL_DWH.DDL_QUERY)
    db_conn.commit()
    print("Base de Datos Creada Exitosamente")
except Exception as ex:
    print("ERROR: Error al crear la base de datos.")
    print(ex)

# %% [markdown]
# ### CREACION DE LAS DIMENSIONES

# %%
RDS_HOST = 'oltp.cpvsavu5bx5f.us-east-1.rds.amazonaws.com'
#RDS_HOST=config.get('OLTP', 'RDS_HOST')
#RDS_HOST
print(RDS_HOST)

# %%
postgres_driver = f"""postgresql://{config.get('OLTP', 'DB_USER')}:{config.get('OLTP', 'DB_PASSWORD')}@{RDS_HOST}:{config.get('OLTP', 'DB_PORT')}/{config.get('OLTP', 'DB_NAME')}"""  

# %% [markdown]
# #### DIMENSION CLIENTES

# %%
sql_query = 'SELECT * FROM cliente;'
dw_cliente = pd.read_sql(sql_query, postgres_driver)
dw_cliente.head()

# %%
sql_query = 'SELECT * FROM municipio;'
dw_municipio = pd.read_sql(sql_query, postgres_driver)
dw_municipio.head()

# %%
dw_cliente_municipio = dw_cliente.merge(dw_municipio, on='municipio_id', how='inner')
dw_cliente_municipio.head()

# %%
sql_query = 'SELECT * FROM departamento;'
dw_departamento = pd.read_sql(sql_query, postgres_driver)
dw_departamento.head()

# %%
dw_cliente_mun_dep = dw_cliente_municipio.merge(dw_departamento, left_on='departamento_id_x', 
                                                                    right_on='departamento_id',  
                                                                    how='inner')
dw_cliente_mun_dep.head()

# %%
sql_query = 'SELECT * FROM region;'
dw_region = pd.read_sql(sql_query, postgres_driver)
dw_region.head()

# %%
dw_cliente_region = dw_cliente_mun_dep.merge(dw_region, left_on='region_id_x', 
                                                                    right_on='region_id',  
                                                                    how='inner')
dw_cliente_region.head()

# %%
sql_query = 'SELECT * FROM segmento;'
dw_segmento = pd.read_sql(sql_query, postgres_driver)
dw_segmento.head()

# %%
dim_cliente = dw_cliente_region.merge(dw_segmento, on='segmento_id', how='inner')
dim_cliente.drop(['segmento_id', 'region_id_x', 'departamento_id_x', 'municipio_id', 'departamento_id_y', 'departamento_id', 'region_id_y', 'region_id'], axis=1,  inplace=True)
dim_cliente.head()

# %% [markdown]
# #### DIMENSION PRODUCTO

# %%
sql_query = 'SELECT * FROM producto;'
dw_producto = pd.read_sql(sql_query, postgres_driver)
dw_producto.head()

# %%
sql_query = 'SELECT * FROM subcategoria;'
dw_subcategoria = pd.read_sql(sql_query, postgres_driver)
dw_subcategoria.head()

# %%
dim_producto = dw_producto.merge(dw_subcategoria, on='subcategoria_id', how='inner')
dim_producto.drop(['categoria_id_x', 'subcategoria_id', 'categoria_id_y'], axis=1,  inplace=True)
dim_producto.head()

# %% [markdown]
# #### DIMENSION STORE

# %%
sql_query = 'SELECT * FROM store;'
dw_store = pd.read_sql(sql_query, postgres_driver)
dw_store.head()

# %%
sql_query = 'SELECT * FROM region;'
dw_region = pd.read_sql(sql_query, postgres_driver)
dw_region.head()

# %%
dw_store_region = dw_store.merge(dw_region, on='region_id', how='inner')
dw_store_region.head()

# %%
sql_query = 'SELECT * FROM gerente;'
dw_gerente = pd.read_sql(sql_query, postgres_driver)
dw_gerente.head()

# %%
dim_store = dw_store_region.merge(dw_gerente, on='gerente_id', how='inner')
dim_store.drop(['region_id', 'gerente_id'], axis=1,  inplace=True)
dim_store.head()

# %% [markdown]
# ### DIMENSION CIUDAD

# %%
sql_query = 'SELECT * FROM municipio;'
dw_municipio = pd.read_sql(sql_query, postgres_driver)
dw_municipio.head()

# %%
sql_query = 'SELECT * FROM departamento;'
dw_departamento = pd.read_sql(sql_query, postgres_driver)
dw_departamento.head()

# %%
dw_municipio_dep = dw_municipio.merge(dw_departamento, on='departamento_id', how='inner')
dw_municipio_dep.head()

# %%
sql_query = 'SELECT * FROM region;'
dw_region = pd.read_sql(sql_query, postgres_driver)
dw_region.head()

# %%
dim_municipio = dw_municipio_dep.merge(dw_region, on='region_id', how='inner')
dim_municipio.drop(['region_id', 'departamento_id'], axis=1,  inplace=True)
dim_municipio.head()

# %% [markdown]
# #### DIMENSION FECHA

# %%
sql_query = 'SELECT fecha_pedido FROM ventas;'
dimFecha = pd.read_sql(sql_query, postgres_driver)
dimFecha.head()

# %%
dimFecha['year'] = pd.DatetimeIndex(dimFecha['fecha_pedido']).year
dimFecha['month'] = pd.DatetimeIndex(dimFecha['fecha_pedido']).month
dimFecha['quarter'] = pd.DatetimeIndex(dimFecha['fecha_pedido']).quarter
dimFecha['day'] = pd.DatetimeIndex(dimFecha['fecha_pedido']).day
dimFecha['week'] = pd.DatetimeIndex(dimFecha['fecha_pedido']).week
dimFecha['dayofweek'] = pd.DatetimeIndex(dimFecha['fecha_pedido']).dayofweek
dimFecha['is_weekend'] = dimFecha['dayofweek'].apply(lambda x: 1 if x > 5 else 0)
dimFecha.head()

# %%
dimFecha['id_fecha'] = dimFecha['year'].astype(str) + dimFecha['month'].astype(str)
dimFecha['id_fecha'] = dimFecha['id_fecha'].astype(str) + dimFecha['day'].astype(str)
dimFecha.drop_duplicates(dimFecha.columns[~dimFecha.columns.isin(['id_fecha'])], 
                         keep='first', inplace=True)
dimFecha.head()

# %%
dim_fecha = dimFecha.drop_duplicates(subset=['id_fecha'])
dim_fecha.head()


# %% [markdown]
# #### TABLA DE HECHOS

# %%
sql_query = '''SELECT v.pedido_id, v.fecha_pedido, v.envio_id, v.cliente_id, c.municipio_id, c.departamento_id, c.region_id,  
v.store_id, v.producto_id, v.total, v.cantidad, v.descuento, v.ganancia
FROM ventas as v
inner join cliente as c on v.cliente_id = c.cliente_id
inner join store as s on v.store_id = s.store_id;'''
df_factTable = pd.read_sql(sql_query, postgres_driver)
df_factTable.head()

# %%
df_factTable['year'] = pd.DatetimeIndex(df_factTable['fecha_pedido']).year
df_factTable['month'] = pd.DatetimeIndex(df_factTable['fecha_pedido']).month
df_factTable['quarter'] = pd.DatetimeIndex(df_factTable['fecha_pedido']).quarter
df_factTable['day'] = pd.DatetimeIndex(df_factTable['fecha_pedido']).day
df_factTable['id_fecha'] = df_factTable['year'].astype(str) + df_factTable['month'].astype(str)
df_factTable['id_fecha'] = df_factTable['id_fecha'].astype(str) + df_factTable['day'].astype(str)
df_factTable.head()

# %%
df_factTable.drop(['year', 'month', 'quarter', 'day'], axis=1, inplace=True)
df_factTable.head()

# %% [markdown]
# #### INSERCION DE DATOS HACIA LAS DIMENSIONES

# %%
def DWHinsertDataToSQL(data_dict, table_name):
     postgres_driver1 = f"""postgresql://{config.get('DWH', 'DB_USER')}:{config.get('DWH', 'DB_PASSWORD')}@{DWH_HOST}:{config.get('DWH', 'DB_PORT')}/{config.get('DWH', 'DB_NAME')}"""    
     df_data = pd.DataFrame.from_records(data_dict)
     try:
          response = df_data.to_sql(table_name, postgres_driver1, index=False, if_exists='append')
          print(f'Se han insertado {response} nuevos registros.' )
     except Exception as ex:
          print(ex)

# %%
DWHinsertDataToSQL(dim_municipio, 'municipio')

# %%
DWHinsertDataToSQL(dim_producto, 'producto')

# %%
DWHinsertDataToSQL(dim_store, 'store')

# %%
DWHinsertDataToSQL(dim_cliente, 'cliente')

# %%
DWHinsertDataToSQL(dim_fecha, 'fecha')

# %%
DWHinsertDataToSQL(df_factTable, 'fact_table')

# %% [markdown]
# ## ANALITICA DEL PROYECTO

# %%
postgres_driver1 = f"""postgresql://{config.get('DWH', 'DB_USER')}:{config.get('DWH', 'DB_PASSWORD')}@{DWH_HOST}:{config.get('DWH', 'DB_PORT')}/{config.get('DWH', 'DB_NAME')}"""

# %% [markdown]
# #### GANANCIAS OBTENIDAS POR ANIO

# %%
dwh_query = '''SELECT f.year, sum(v.ganancia) as ganancias, sum(v.cantidad) as ventas
FROM fact_table as v
inner join fecha as f on v.id_fecha = f.id_fecha
group by f.year;'''
analisis1 = pd.read_sql(dwh_query, postgres_driver1)
analisis1.head()

# %%
analisis1.plot('year', 'ganancias')

# %%
dwh_query = '''SELECT  m.municipio, m.departamento, v.ganancia, v.cantidad
FROM fact_table as v
inner join fecha as f on v.id_fecha = f.id_fecha
inner join municipio as m on v.municipio_id = m.municipio_id;'''
analisis2 = pd.read_sql(dwh_query, postgres_driver1, index_col='municipio')
analisis2.head()

# %%
import plotly.express as px

# %%
fig = px.treemap(analisis2, path=[px.Constant("VENTAS POR DEPARTAMENTO Y MUNICIPIO"), 'departamento','cantidad'],
color_continuous_scale='RdBu')
fig.show()

# %%
dwh_query = '''SELECT p.subcategoria, sum(v.ganancia) as ganancias
FROM fact_table as v
inner join producto as p on v.producto_id = p.producto_id
group by p.subcategoria;'''
analisis3 = pd.read_sql(dwh_query, postgres_driver1, index_col="subcategoria")
analisis3

# %%
analisis3.plot(kind = 'barh')


# %%
dwh_query = '''select p.nombre_del_producto, sum(v.ganancia)/sum(cantidad) as ganancia_neta
FROM fact_table as v
inner join producto as p on v.producto_id = p.producto_id
group by p.nombre_del_producto
order by ganancia_neta asc 
FETCH FIRST 20 ROWS only;'''
analisis4 = pd.read_sql(dwh_query, postgres_driver1, index_col='nombre_del_producto')
analisis4.head()

# %%
analisis4.plot(kind = 'barh')

# %%
dwh_query = '''select p.nombre_del_producto, sum(v.ganancia)/sum(cantidad) as ganancia_neta
FROM fact_table as v
inner join producto as p on v.producto_id = p.producto_id
group by p.nombre_del_producto
order by ganancia_neta desc 
FETCH FIRST 20 ROWS only;'''
analisis5 = pd.read_sql(dwh_query, postgres_driver1, index_col='nombre_del_producto')
analisis5.head()

# %%
analisis5.plot(kind = 'barh')


