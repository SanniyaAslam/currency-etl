FROM astrocrpublic.azurecr.io/runtime:3.3-9

# ---- Meta details ----
LABEL org.opencontainers.image.title="currency-etl-airflow" \
      org.opencontainers.image.description="Airflow ETL: Frankfurter API -> MongoDB" \
      docker.networking="Joins the 'airflow' bridge network; MongoDB reachable at host 'mongo:27017', Mongo Express at 'mongo-express:8081'" \
      docker.persistent.storage="MongoDB data persisted in named volume 'mongo_data' mounted at /data/db"
