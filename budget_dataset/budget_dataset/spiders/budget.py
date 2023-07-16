import scrapy
from scrapy import FormRequest
import pandas as pd
import re

project_path = '/Users/tnluser/airflow/dags/budget_dataset'

class BudgetSpider(scrapy.Spider):
    name = "budget"
    start_urls = ['https://himkosh.nic.in/eHPOLTIS/PublicReports/wfrmBudgetAllocationbyFD.aspx']

    def parse(self, response):
        data = {
            "ctl00$MainContent$txtFromDate": "01/04/2018",
            "ctl00$MainContent$txtQueryDate": "31/03/2022",
            "ctl00$MainContent$ddlQuery": "DmdCd,HOA",
            "ctl00$MainContent$rbtUnit": "0",
            "ctl00$MainContent$btnGetdata": "View Data"
        }
        ''' All the other params which might be required like __VIEWSTATE ,__VIEWSTATEGENERATOR will be taken care of automatically by fromrequest 
            Once the response is ready it will call the parse_table function '''
        yield FormRequest.from_response(response, formdata=data, callback=self.parse_table)

    def parse_table(self, response):
        dfs = pd.read_html(response.text, header=2) # Converts the html tables to dataframe
        df = dfs[0]
        df = clean_dataframe(df)
        df.to_csv(f'{project_path}/files/budget_csv.csv',index=False)


def clean_dataframe(df):
    # Remove all chars from the col names
    columns = []
    for col_name in df:
        # Checking all chars in each col and removes all non alphabets by replacing those with empty values=.
        columns.append(re.sub(r'[^a-zA-Z]', '', col_name))
    df.columns = columns

    # Dropping all rows where Dmdcd value is Grand Total by fetching by indices and replaces the
    df = df.drop(df[df['DmdCd'] == 'Grand Total'].index)

    # Fetching the rows where HOA equals to Total
    fil_df = df[df["HOA"] == "Total"]

    # Fetching Dmdcd column values as all other rows were empty
    dmd_lt = list(fil_df['DmdCd'])
    dmd_lt.sort()  # Sorting them as it have a demand code prefix .which we can use for mapping.
    df = df.drop(df[df['HOA'] == 'Total'].index) # Dropping off all the rows where HOA equals to Total

    #Mapping Dmdcd column value on each row by using the prefix value of HOA with respect to the index of dmd_lt list
    df['DmdCd'] = df['HOA'].apply(lambda x: dmd_lt[int(x.split('-')[0]) - 1])

    # Listing out the col names which we want to add after splitting DmdCd and HOA .
    dmdcd_sub_cols = ['DemandCode', 'Demand']
    hoa_sub_cols = ['MajorHead', 'SubMajorHead', 'MinorHead', 'SubMinorHead', 'DetailHead', 'SubDetailHead', 'BudgetHead','PlanNonPlan', 'VotedCharged', 'StatementofExpenditure']

    df[dmdcd_sub_cols] = df["DmdCd"].str.split("-", n=1, expand=True)
    df[hoa_sub_cols] = df["HOA"].str.split("-", n=9, expand=True)

    return df

