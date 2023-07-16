import sqlite3
import pandas as pd


def df_to_sqllite(project_path):
    # Connects to database if it's present or else a new db with the provided name will be created
    conn = sqlite3.connect(f'{project_path}/database/hp_treasury.db')

    cursor = conn.cursor()

    sql_table_create = '''CREATE TABLE IF NOT EXISTS TREASURY_EXPENDITURE (DmdCd VARCHAR,HOA VARCHAR,SanctionBudgetApril FLOAT,Addition FLOAT,Saving FLOAT,RevisedBudgetA FLOAT,ExpenditurewithinselectedperiodB FLOAT,BalanceAB FLOAT,DemandCode INT,Demand VARCHAR,MajorHead INT,SubMajorHead INT,MinorHead INT,SubMinorHead INT,DetailHead VARCHAR,SubDetailHead VARCHAR,BudgetHead INT,PlanNonPlan CHAR,VotedCharged CHAR,StatementofExpenditure VARCHAR) '''

    cursor.execute(sql_table_create)

    df = pd.read_csv(f'{project_path}/files/budget_csv.csv')  # Reads the csv files which was created after scrapping and convert it to dataframe

    df.to_sql(name='TREASURY_EXPENDITURE',con=conn,if_exists='append',index=False) # Loads data to the treasury_expenditure


    conn.close()

