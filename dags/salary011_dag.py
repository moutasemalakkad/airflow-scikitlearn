import pandas as pd
import numpy as np
import json
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from airflow.operators.python import task, get_current_context
import pickle


class NumpyArrayEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NumpyArrayEncoder, self).default(obj)


def read_dataset(path) -> pd.DataFrame:
    # read the csv file
    salary_df = pd.read_csv(path)
    return salary_df


def split_features_target(data_set, features, target):
    X = data_set[features]
    y = data_set[target]
    return X, y


def test_train_split(X, y, test_size: float):
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size)
    return X_train, X_test, y_train, y_test


def to_numpy(X, y) -> np.array:
    X = np.array(X).astype('float32')
    y = np.array(y).astype('float32')
    return X, y


def evaluate(regression_model_sklearn, X_test, y_test):
    regression_model_sklearn_accuracy = regression_model_sklearn.score(X_test, y_test)
    return regression_model_sklearn_accuracy


def np_to_json(X_train, y_train, X_test, y_test):
    X_train = json.dumps(X_train, cls=NumpyArrayEncoder)
    X_test = json.dumps(X_test, cls=NumpyArrayEncoder)
    y_train = json.dumps(y_train, cls=NumpyArrayEncoder)
    y_test = json.dumps(y_test, cls=NumpyArrayEncoder)
    return X_train, y_train, X_test, y_test


@dag(schedule_interval="@daily", start_date=days_ago(1), catchup=False)
def salary011_dag():
    @task(multiple_outputs=True, do_xcom_push=False)
    def featurized():
        salary_df = pd.read_csv('/usr/local/airflow/salary.csv', encoding='utf8')
        X, y = split_features_target(salary_df, ['YearsExperience'], ['Salary'])
        X, y = to_numpy(X, y)
        X_train, X_test, y_train, y_test = test_train_split(X, y, 0.2)
        X_train, y_train, X_test, y_test = np_to_json(X_train, y_train, X_test, y_test)
        return {'X_train': X_train, 'y_train': y_train, 'X_test': X_test, 'y_test': y_test}

    @task
    def train_model(X_train, y_train):
        regression_model_sklearn = LinearRegression(fit_intercept=True)
        regression_model_sklearn.fit(json.loads(X_train), json.loads(y_train))
        file_path = '/app/clean_data/finalized_model.sav'
        pickle.dump(regression_model_sklearn, open(file_path, 'wb'))
        print(type(regression_model_sklearn))

        return file_path

    @task
    def evaluate(**kwargs):
        ti = kwargs['ti']
        X_test = ti.xcom_pull(dag_id='salary01_dag', task_ids='featurized', key='X_test')
        y_test = ti.xcom_pull(dag_id='salary01_dag', task_ids='featurized', key='y_test')
        file_path = ti.xcom_pull(dag_id='salary01_dag', task_ids='train_model')
        regression_model_sklearn = pickle.load(open(file_path, 'rb'))
        regression_model_sklearn_accuracy = regression_model_sklearn.score(json.loads(X_test), json.loads(y_test))
        print(regression_model_sklearn_accuracy)

    featurized = featurized()
    train_model = train_model(featurized['X_train'], featurized['y_train'])
    evaluate = evaluate()

    featurized >> train_model >> evaluate


salary01 = salary011_dag()
