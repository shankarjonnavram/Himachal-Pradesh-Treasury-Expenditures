# Himachal-Pradesh-Treasury-Expenditures

This project demonstrates scraping data from a website, store it in a CSV file, and then load it into a SQLite database using Python, Scrapy, SQLite, and Apache Airflow.

### About the source
The state of Himachal Pradesh uses an Integrated Financial Management system to manage and disseminate various reports related to the state treasury department. The web platform contains several important reports which one can use to analyze the expenditures of the state government. 

HP Treasury portal : https://himkosh.nic.in/eHPOLTIS/PublicReports/wfrmBudgetAllocationbyFD.aspx

### Project Structure 

The project is organized as follows:

- `budget_dataset/`: Root directory for the project.

    - `airflow_dags/`: This folder contains the Airflow DAG.
    -  `budget_dataset/spider` : Contains the Scrapy spider (`budget.py`) to scrape data from the target website.
    
    - `database/`: This contains the both the database that will be created and the function which loads data from csv to that SQLite database. 
    - `files/`:  Path where csv files are created 

### Airflow DAG Setup 

- Add the `budget_dataset` root directory in the airflow dags path.
- The airflow dag code for our project is present in airflow_dag directory
- We need to change the project_path in `budget_dataset/airflow_dags/web_to_sqldb_pipeline.py` python file to dags path by adding project_name(`budget_dataset`) in the path
- Also , we need to provide the same path for `project_path` variable in `budget_dataset/budget_dataset/spiders/budget.py` file in spiders directory
