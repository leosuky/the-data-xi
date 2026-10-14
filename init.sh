#!/bin/bash
dbt deps --project-dir /usr/local/airflow/the_data_xi_dbt
exec "$@"
