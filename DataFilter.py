import sqlite3
import pandas as pd

class CleanCompanyNames:

    def __init__(self, *args, **kwargs):
        self.con = None

    def connect_to_db(self):
        self.con = sqlite3.connect("semos_company_names.db")

    def purify_data(self):

        self.connect_to_db()

        df = pd.read_sql('''SELECT * FROM companies''', self.con)
        df['company_name_cleaned'] = df['name'] \
            .replace({'LTD': ''}, regex=True) \
            .replace({'LIMITED': ''}, regex=True) \
            .replace({'LLP': ''}, regex=True) \
            .replace({'LIABILITY': ''}, regex=True) \
            .replace({'PARTNERSHIP': ''}, regex=True) \
            .replace({'-THE$': ''}, regex=True) \
            .replace({'- THE$': ''}, regex=True) \
            .replace({'""$': ''}, regex=True) \
            .replace({'"': ' '}, regex=True) \
            .replace({'.$': ''}, regex=True) \
            .replace({'#': ''}, regex=True) \
            .replace({'/?/': ''}, regex=True) \
            .replace({'\[.*\]': ''}, regex=True) \
            .replace({'\(.*\)': ''}, regex=True) \
            .replace({'\(.*': ''}, regex=True) \
            .replace({'\s+': ' '}, regex=True)

        df.to_sql('companies', self.con, chunksize=1000, if_exists='replace', index=False)



    def run_program(self):

        self.purify_data()
        print(f"\nPurification of big data finished")




    def print_list(self):

        self.connect_to_db()

        df1 = pd.read_sql('''SELECT * FROM companies''', self.con, chunksize=1000)

        for chunk in df1:
            for row in chunk.values:
                print(str(row).replace('[', '').replace(']', ''))

        self.con.close()


def main():
    ccn = CleanCompanyNames()
    ccn.run_program()
    ccn.print_list()