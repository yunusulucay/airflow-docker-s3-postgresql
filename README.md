# Apache Airflow on Docker With AWS S3 and PostgreSQL

![airflow-docker-s3-psql2](https://user-images.githubusercontent.com/42489236/188277092-07580a41-e2b4-4897-9762-596c0c419098.jpg)


First step create a S3 bucket. After that fill the configuration file. And download the repo.

After you download the repo. Put the files to a folder named end_to_end_project . Because Docker give names a prefix which the parent folder has. In the same directory in CLI run ```docker-compose up -d``` .

After 2-3 minutes go to ```localhost:8080``` and login. Username = admin, password = admin .

Make DAGs unpause. Trigger get_api_send_psql DAG.

Article : https://medium.com/p/af29a08213a1/edit
