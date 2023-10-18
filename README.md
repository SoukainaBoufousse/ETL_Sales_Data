# ETL_Sales_Data
## Project Overview
This repository contains a mini project demonstrating an ETL (Extract, Transform, Load) pipeline for processing sales data using Docker and Apache Airflow for orchestration.
## Prerequisites
- Docker and Docker Compose installed
- Python 3.x
- Postgresql (you can use [pgAdmin4](https://www.pgadmin.org/) or [DBeaver](https://dbeaver.io/) )
## Data Pipeline
![etl_pipeline](https://github.com/SoukainaBoufousse/ETL_Sales_Data/assets/104233981/066f6066-bc31-4977-9402-03988c4324e4)

## Repository Structure

In this structure:
- `/docker-compose` contains the Docker Compose file for defining and running the Docker containers.
- `/etl_script` contains the Python script for the ETL pipeline (`pipeline.py`).
- `/data` contains the input data files (`Train.csv` and `Test.csv`).
- `Dockerfile` specifies how to build the Docker image for the ETL pipeline.
- `requirements.txt` lists the Python dependencies needed to run the ETL pipeline.
- `README.md` provides information about the project and how to set it up.


