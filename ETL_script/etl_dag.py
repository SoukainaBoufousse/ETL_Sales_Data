from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 15), # Change the date to the actual start date
    'retries': 1,
}

dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='ETL DAG to load sales data into PostgreSQL',
    schedule_interval='@once',
)

import os
dag_path = os.getcwd()

def extract_data():
    import pandas as pd
    train_df = pd.read_csv(f"{dag_path}/dags/Train.csv")
    test_df = pd.read_csv(f"{dag_path}/dags/Test.csv")
    return train_df, test_df

def impute_missing_values(data):
    item_avg_weight = data.pivot_table(values='Item_Weight', index='Item_Identifier')
    missing_weight = data['Item_Weight'].isnull()
    data.loc[missing_weight, 'Item_Weight'] = data.loc[missing_weight, 'Item_Identifier'].apply(lambda x: item_avg_weight.at[x, 'Item_Weight'])
    from scipy.stats import mode
    outlet_size_mode = data.pivot_table(values='Outlet_Size', columns='Outlet_Type', aggfunc=lambda x: mode(x.astype('str')).mode[0])
    missing_size = data['Outlet_Size'].isnull()
    data.loc[missing_size, 'Outlet_Size'] = data.loc[missing_size, 'Outlet_Type'].apply(lambda x: outlet_size_mode[x])
    visibility_avg = data.pivot_table(values='Item_Visibility', index='Item_Identifier')
    zero_visibility = data['Item_Visibility'] == 0
    data.loc[zero_visibility, 'Item_Visibility'] = data.loc[zero_visibility, 'Item_Identifier'].apply(lambda x: visibility_avg.at[x, 'Item_Visibility'])
    return data

def transform_data(**kwargs):
    ti = kwargs['ti']
    train_df, test_df = ti.xcom_pull(task_ids='extract_data_task')  
    train_df['source'] = 'df_train'
    test_df['source'] = 'df_test'
    test_df['Item_Outlet_Sales'] = 0
    import pandas as pd
    data = pd.concat([train_df, test_df], sort=False)
    transformed_data = impute_missing_values(data)
    return transformed_data

def load_to_postgres(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data')
    
    # Create a list of tuples containing the parameters for the SQL query
    # Each tuple should correspond to a row of data
    parameters_list = [tuple(row) for row in transformed_data.values]
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    
    # Load data into PostgreSQL table
    pg_hook.insert_rows(
        table="etl",
        rows=parameters_list,
        columns=[
            'item_identifier', 'item_weight', 'item_fat_content', 'item_visibility',
            'item_type', 'item_mrp', 'outlet_identifier', 'outlet_establishment_year',
            'outlet_size', 'outlet_location_type', 'outlet_type', 'item_outlet_sale', 'source'
        ],
        replace=False  # Set to True if you want to replace existing rows
    )


# Define the tasks
extract_data_task = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract_data,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,  # Provide the task context to the callable
    dag=dag,
)


load_to_postgres_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
extract_data_task >> transform_data_task >> load_to_postgres_task
