"""
fetch_flight_files
DAG auto-generated by Astro Cloud IDE.
"""

from airflow.decorators import dag
from astro import sql as aql
from astro.sql.table import Table
import pandas as pd
import pendulum


@aql.dataframe(task_id="train_model")
def train_model_func(merge_rows: pd.DataFrame):
    # print(merge_rows.shape)
    model_data_clean = merge_rows.dropna()
    
    #model_data_clean[['year','month','day']] = model_data_clean['fl_date'].astype(str).str.split('-',expand=True)
    #model_data_clean['crs_dep_hour'] = model_data_clean['crs_dep_time'].astype(str).str.strip(-2).astype(str)
    #model_data_clean['crs_arr_hour'] = model_data_clean['crs_arr_time'].astype(str).str.strip(-2).astype(str)
    print(model_data_clean.columns)
    
    X = model_data_clean[[
        'op_carrier',
        'op_carrier_fl_num',
        'origin',
        'dest',
        'crs_elapsed_time',
        'distance',
        # 'month',
        # 'day',
        # 'crs_dep_hour',
        # 'crs_arr_hour'
    ]]
    
    y = model_data_clean[['cancelled']]
    
    categorical_cols = [
        'op_carrier',
        'op_carrier_fl_num',
        'origin',
        'dest',
        # 'month',
        # 'day',
        # 'crs_dep_hour',
        # 'crs_arr_hour'
    ]
    
    mlflow.set_tracking_uri("http://astro.fletcher.za.net:5000")
    mlflow.set_experiment("xgboost_model")
    mlflow.xgboost.autolog(registered_model_name="xgboost_model")
    #with mlflow.start_run(run_name=model_directory) as run:
        #mlflow.xgboost.autolog()
    
        #mlflow.log_param("model_directory",model_directory)
    
    ct = ColumnTransformer(
        [('le', OneHotEncoder(), categorical_cols)],
        remainder='passthrough'
    )
    
    X_trans = ct.fit_transform(X)
    print(type(X_trans))
    print(X_trans.shape)
    
    
    X_train, X_test, y_train, y_test = train_test_split(X_trans, y, random_state=42)
    xgbclf = xgboost.XGBClassifier() 
    
    pipe = Pipeline([('scaler', StandardScaler(with_mean=False)),
            ('xgbclf', xgbclf)])
    
    pipe.fit(X_train, y_train)
    
    test_score = pipe.score(X_test, y_test)
    return test_score
    
    #model_uri = "runs:/{}".format(run.info.run_id)
    #mlflow.sklearn.log_model(pipe,"test_model")
    #mlflow.register_model(model_uri=model_uri, name="XGBClassifier",tags={
    #        'pipe':f's3://{bucketname}/models/{model_directory}/pipe.joblib',
    #        'ct':f's3://{bucketname}/models/{model_directory}/ct.joblib',
    #        })
    #mlflow.log_artifact(f's3://{bucketname}/models/{model_directory}/pipe.joblib')    
    

@aql.transform(conn_id="snowflake_jeffletcher", task_id="fetch_cancelled_flight_data")
def fetch_cancelled_flight_data_func():
    return """select FL_DATE, OP_CARRIER, OP_CARRIER_FL_NUM, ORIGIN, DEST, CRS_DEP_TIME, CRS_ARR_TIME, CRS_ELAPSED_TIME, DISTANCE, CANCELLED from flight_data_2 where cancelled = 1"""

@aql.dataframe(task_id="cell_1")
def cell_1_func():
    import os
    print(os.environ["AIRFLOW__CORE__ENABLE_XCOM_PICKLING"])

@aql.transform(conn_id="snowflake_jeffletcher", task_id="cancelled_flights_count")
def cancelled_flights_count_func():
    return """select count(*) as cancelled_flights from flight_data_2 where cancelled = 1"""

@aql.dataframe(task_id="df_to_int")
def df_to_int_func(cancelled_flights_count: pd.DataFrame):
    return cancelled_flights_count.iloc[0]['cancelled_flights']

@aql.transform(conn_id="snowflake_jeffletcher", task_id="fetch_normal_flight_data_sample")
def fetch_normal_flight_data_sample_func(df_to_int: Table):
    return """select FL_DATE, OP_CARRIER, OP_CARRIER_FL_NUM, ORIGIN, DEST, CRS_DEP_TIME, CRS_ARR_TIME, CRS_ELAPSED_TIME, DISTANCE, CANCELLED from flight_data_2 sample ({{df_to_int}} rows) where cancelled = 0"""

@aql.dataframe(task_id="merge_rows")
def merge_rows_func(fetch_cancelled_flight_data: pd.DataFrame, fetch_normal_flight_data_sample: pd.DataFrame):
    print(type(fetch_cancelled_flight_data))
    print(type(fetch_normal_flight_data_sample))
    concat_rows = pd.concat([fetch_normal_flight_data_sample, fetch_cancelled_flight_data])
    print(len(concat_rows))
    return concat_rows

@dag(
    schedule_interval=None,
    start_date=pendulum.from_format("2022-12-20", "YYYY-MM-DD"),
)
def fetch_flight_files():
    fetch_cancelled_flight_data = fetch_cancelled_flight_data_func()

    cancelled_flights_count = cancelled_flights_count_func()

    df_to_int = df_to_int_func(
        cancelled_flights_count,
    )

    fetch_normal_flight_data_sample = fetch_normal_flight_data_sample_func(
        df_to_int,
    )

    merge_rows = merge_rows_func(
        fetch_cancelled_flight_data, fetch_normal_flight_data_sample,
    )

    train_model = train_model_func(
        merge_rows,
    )

    cell_1 = cell_1_func()

    df_to_int << cancelled_flights_count

    fetch_normal_flight_data_sample << df_to_int

    merge_rows << [fetch_cancelled_flight_data, fetch_normal_flight_data_sample]

    train_model << merge_rows

dag_obj = fetch_flight_files()
