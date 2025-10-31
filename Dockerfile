FROM bitnami/airflow:latest
# RUN pip install git+https://github.com/everapihq/freecurrencyapi-python.git
RUN pip install freecurrencyapi
RUN pip install apache-airflow-providers-google
RUN pip install psycopg2-binary
RUN pip install sqlalchemy
RUN pip install google-cloud-secret-manager==2.7.2
RUN pip install google-auth==2.13.0
RUN pip install google-auth-oauthlib==0.7.0
RUN pip install google-auth-httplib2==0.1.0
RUN pip install google-cloud-bigquery==3.5.0
RUN pip install pandas==2.2.2
RUN pip install flask==1.1.4
RUN pip install werkzeug==1.0.1
RUN pip install markupsafe==2.0.1
RUN pip install pandas-gbq==0.19.0
RUN pip install colorama
# RUN pip install --no-cache-dir apache-airflow-providers-postgres psycopg2-binary
