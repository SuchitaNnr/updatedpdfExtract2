import pdfplumber
import re
import pandas as pd
import pyodbc
import pandas
from itertools import combinations
from dateutil import parser
import log
import logging
from connect import Connect  # module for creating the database connection.


class PdfExtractor:
    def __init__(self, inputfile):
        self.pdf = pdfplumber.open(inputfile)
        self.conobj = Connect()
        self.cnxn = self.conobj.connectdb()  # connection to db initiated

    def combs(self, lst, n):
        return (c for k in range(1, n + 1) for c in combinations(lst, k))

    # def best_match(lst,target, n=10):
    #    s = min(combs(lst, n), key=lambda c: (abs(float(target) - sum(c)), len(c)))
    # print (s,target)
    #   return [x for x in s]
    def best_match(self,lst,target, n=10):
        #s = min(combs(lst, n), key=lambda c: (abs(float(target) - sum(c)), len(c)))
        #print('in best_match:',type(lst),type(target))
        target_float=float(target)
        s=[]
        if target_float>0:
            if len(lst)>=0 and sum(lst)>=target_float :
                minimum=-99999999
                for k in self.combs(lst, n):
                    diff=target_float - sum(k)
                    if diff<=0 and minimum<diff:
                        s=k
                        minimum=diff
                #print (s,target,diff)
                
            else:
                s = min(self.combs(lst, n), key=lambda d: (abs(target_float - sum(d)), len(d)))
        #     #print(s)
        return [float(x) for x in s]

    def cost_control(self, shipment_id):
        sqlcommand = f"""
        SET NOCOUNT ON
        EXEC [dbo].[datawindow_shipment_summary_master_cost_only]
            @shipment_id = '{shipment_id}'

        """

        df = pandas.read_sql_query(sqlcommand, con=self.cnxn)
        #print('sqloutput:',sqlcommand, df)
        df = df[df['voided_flag'] == 0]  # filter out voide
        df = df[df['vendor_invoice_no'].isnull()]  # filter out allocated cost
        cost = df['est_cost_amount'].to_list()  # return list
        if not cost:  # check if list is empty and return 0
            cost = [0]
        #print('sqloutput cost_control output:',cost)
        return (cost)

    def shipment_id(self, mawb):

        cursor = self.cnxn.cursor()
        sqlcommand = f"""
        SELECT [shipment_id],[file_no]
        FROM [devdb].[dbo].[shipment]
        WHERE bill_no = '{mawb}'
        ORDER BY shipment_date desc
        """
        cursor.execute(sqlcommand)

        for row in cursor.fetchall():
            return str(row[0]), str(row[1])

    def parse_row_airline(self, first_line):
        return {
            "prefix": first_line[:4].strip()
        }

    def parse_row(self, first_line):
        # amount = first_line[118:132].strip()
        # if amount.endswith("-"):
        #     amount = amount.replace("-","")
        #     amount = "-"+amount

        return {
            "bill": first_line[:9].strip(),
            "amount": first_line[118:132].strip()

        }

    def extractData(self, outfile):
        df1 = []
        for x in range(0, len(self.pdf.pages)):
            try:
                print (f"Page no: {x}")
                page = self.pdf.pages[x]

                text = page.extract_text()
                prefix = re.compile(r"AIRLINE:(.*)\s+TAX POINT DATE", re.DOTALL)
                prefix_search = re.search(prefix, text).group(1)
                line = prefix_search.split("\n")
                #print(line)
                core_pat = re.compile(r"RATED[\=\s]+(.*)\n\s+SUB TOTAL", re.DOTALL)
                core = re.search(core_pat, text).group(1)
                lines = core.split("\n")
                # line_groups = list(zip(lines[::2], lines[1::2]))
                #print(lines)
                invoice_re = re.compile(r"INVOICE NR:[\=\s]+(.*)\n\s+INVOICE DATE:", re.DOTALL)
                invoice_no = re.search(invoice_re, text).group(1)
                invoice_no1 = invoice_no.split("\n")
                #print(invoice_no1)
                invoice_date_re = re.compile(r"INVOICE DATE:[\=\s]+(.*)\n\s+AGENT:", re.DOTALL)
                invoice_date_no = re.search(invoice_date_re, text).group(1)
                invoice_date = invoice_date_no.split("\n")
                invoice_date_formated = parser.parse(invoice_date[0]).date()
                
                p = [self.parse_row_airline(first_line)
                     for first_line in line]
                parsed = [self.parse_row(first_line)
                          for first_line in lines]

                columns = list(parsed[0].keys())
                # print (p[0]['prefix'])
                df = pd.DataFrame(parsed)[columns]
                df.loc[df["amount"].str.endswith("-"), "amount"] = (
                        "-" + df.loc[df["amount"].str.endswith("-"), "amount"].str.strip("- "))
                # df['prefix'] = p[0]['prefix']
                df['MAWB'] = p[0]['prefix'] + "-" + df['bill']
                df['invoice_no'] = invoice_no1[0]
                df['invoice_date'] = invoice_date_formated
                df['page'] = x + 1
                for index, row in df.iterrows():
                    shipment = self.shipment_id(row['MAWB'])
                    df.at[index, 'shipment_id'] = shipment[0]
                    df.at[index, 'file_ref'] = shipment[1]
                df['cost_list'] = pd.Series(dtype='object')
                df['match'] = pd.Series(dtype='object')

                for index, row in df.iterrows():
                    df.at[index, 'cost_list'] = self.cost_control(row['shipment_id'])
                    #print('row(costList):',row['cost_list'])
                for index, row in df.iterrows():
                    #print(type(row['cost_list']),row['amount'])    
                    df.at[index, 'match'] = self.best_match(row['cost_list'], row['amount'])
                #print(df)
                for index, row in df.iterrows():
                    # if sum(row['match']) - float(row['amount']) <0:
                    #     for x in df.at[index,'cost_list']:
                    #         if x - df.at[index,'amount'] > 0:
                    #             df.at[index,'difference'] = x
                    #         else:
                    # print(row['match'],type(row['match']))
                    df.at[index, 'difference'] = round(float(sum(row['match'])) - float(row['amount']),2)
                df1.append(df)
                # print(df1)
            except AttributeError as e:
                print(e)
                pass
            #except Exception as e:
                #logging.error(e)
            #    print(e)
            #    pass
        try:     
            df1 = pd.concat(df1)
            df1['difference'] = pd.to_numeric(df1['difference']).round(2)
        except Exception as e:
            print(e) 

        df1.to_csv(outfile)
        #print(df)


# A11234724
inputfile = 'C:/Dev/cass rpa/May/May22 ManExp P2.pdf'
outfile = 'C:/Dev/cass rpa/May/May22 ManExp P2.csv'
#inputfile = 'AprP2_lhr.pdf'
#outfile = 'AprP2_lhr.csv'
l = log.Log()
pdfobj = PdfExtractor(inputfile)
pdfobj.extractData(outfile)
