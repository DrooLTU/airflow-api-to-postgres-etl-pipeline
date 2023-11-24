# Precious metals price ML model trainer

Airflow ETL pipeline for loan data ETL.


## Tech Stack

- Apache Airflow
- Docker
- Kaggle API


## What it does

- Fetches data from predefined Kaggle repository, transforms it and stores in in a postgreSQL DB.
 

## Getting Started

To run this project locally, follow these steps:

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/TuringCollegeSubmissions/jukaral-DE3.1.5git
   ```

2. **Navigate to the Project Directory:**

    ```bash
    cd jukaral-DE3.1.5
    ```

3. **Start the Docker Containers:**
    - Run the following command in your terminal, from the root of the project:

    ```bash
    docker compose up
    ```

4. **Variables and connections:**

    - Create a Postgres connection with the following:
        - Conn id: ```MainPG```
        - Database: ```airflow```
        - Host: ```postgres```
        - Port: ```5432```
        - Enter your username and password you set up in the .env

    - Create a Postgres connection with the following:
        - Conn id: ```LoanDB```
        - Database: ```loans```
        - Host: ```postgres```
        - Port: ```5432```
        - Enter your username and password you set up in the .env

    - Import the provided default 'variables.json' to your Airflow Variables for quick start.
    You'll have to edit them as needed.
