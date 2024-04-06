import os

local_path = "/opt/airflow/dags"


configs = {
    "meta_path": f"{local_path}/scripts/metadado.xlsx",
    "raw_path": f"{local_path}/data/raw/raw_",
    "work_path": f"{local_path}/data/work/work_cadastro.csv",
}