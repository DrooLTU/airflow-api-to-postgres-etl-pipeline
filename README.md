# Precious metals price ML model trainer

Airflow ETL pipeline for loan data.


## Tech Stack

- Apache Airflow
- Docker


## What it does

- 


## How it works (brief)

1. 


## How it works (in-detail)

1. The data fetching/ETL DAG:
    
 

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
    - With the container running - create the default filesystem (fs) connection with the following:
        - Conn id: ```fs_default```
        - Leave everything else blank.

    - Then create a Postgres connection with the following:
        - Conn id: ```MainPG```
        - Host: ```postgres```
        - Port: ```5432```
        - Enter your username and password you set up in the .env

    - Import the provided default 'variables.json' to your Variables for quick start.
    You'll have to edit them as needed.
