import findspark
findspark.init()
from pyspark.sql import SQLContext, HiveContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import udf, col, months_between, floor
from pyspark.sql.types import TimestampType
import requests
import pandas as pd
import numpy as np
from io import StringIO
from datetime import datetime
from statsmodels.tsa.stattools import adfuller

conf = SparkConf().setMaster("yarn").set('spark.sql.warehouse.dir' , '/hive/warehouse')
sc = SparkContext(conf = conf)
#hive_context = HiveContext(sc)

sqlContext = HiveContext(sc)

#Import ABT
print('2. Creating ABT and collecting Macroeconomic Data')
print('2.1 Reading in cleaned data')


#Import MacroEconomic Variables
def Str_2_Dte(x):
    return(datetime.strptime(x , '%Y-%m-%d'))

print('2.2 Extract Macroeconomic variables from FRED')
Unemp_link = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=748&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=UNRATE&scale=left&cosd=1948-01-01&coed=2020-06-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Monthly&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2020-07-28&revision_date=2020-07-28&nd=1948-01-01'
GDP_link = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1168&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=GDPC1&scale=left&cosd=1947-01-01&coed=2020-04-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Quarterly&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2020-08-09&revision_date=2020-08-09&nd=1947-01-01'
Inflation_Link = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1168&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=CPIAUCSL&scale=left&cosd=1947-01-01&coed=2020-06-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Monthly&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2020-08-10&revision_date=2020-08-10&nd=1947-01-01'
HPI_Link = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1168&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=CSUSHPISA&scale=left&cosd=1987-01-01&coed=2020-05-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Monthly&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2020-08-10&revision_date=2020-08-10&nd=1987-01-01'
Disp_Inc_Link = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1168&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=DSPIC96&scale=left&cosd=1959-01-01&coed=2020-06-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Monthly&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2020-08-10&revision_date=2020-08-10&nd=1959-01-01'
Employment_Link = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1168&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=PAYEMS&scale=left&cosd=1939-01-01&coed=2020-07-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Monthly&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2020-08-10&revision_date=2020-08-10&nd=1939-01-01'

def Get_MEV(
        link            #The Link to Fred
    ,   transf = 'None' #The tranfomration to ensure stationarity
    ,   lags = 0        #The max number of lags to use
    ,   mth_align = 0   #Whether to shifts the months
):
    In_MEV = requests.get(link)
    In_MEV = In_MEV.content
    In_MEV = In_MEV.decode('utf-8')
    In_MEV = pd.read_csv(StringIO(In_MEV),sep = ',')
    In_MEV['DATE'] = In_MEV['DATE'].apply(Str_2_Dte)
    In_MEV['DATE'] = In_MEV['DATE'] + pd.DateOffset(months=mth_align) + pd.offsets.MonthEnd(0)
    In_MEV = In_MEV[In_MEV['DATE'].dt.month.isin([3,6,9,12])]
    #In_MEV = In_MEV.reset_index()
    In_MEV = In_MEV.sort_values(['DATE'])

    Transf_Index = {'YoY':4,'QoQ':1}

    cols = np.array(In_MEV.columns.tolist())
    cols = cols[cols != 'DATE']
    if transf != 'None':
        for i in cols:
            In_MEV['{}_{}'.format(i,transf)] = In_MEV['{}'.format(i)]/In_MEV['{}'.format(i)].shift(Transf_Index[transf]) - 1
            In_MEV['{}_{}'.format(i,transf)] = In_MEV['{}_{}'.format(i,transf)].round(decimals=8) #Rounding to save storage
            In_MEV = In_MEV.drop(columns=['{}'.format(i)])

    cols = np.array(In_MEV.columns.tolist())
    cols = cols[cols != 'DATE']
    for i in cols:
        MEV_vec = np.array(In_MEV[i].values)
        MEV_vec = MEV_vec[~np.isnan(MEV_vec)]
        result = adfuller(MEV_vec)
        print('ADF Test for {} variable'.format(i))
        print('ADF Statistic: %f' % result[0])
        print('p-value: %f' % result[1])

    if lags > 0:
        cols = np.array(In_MEV.columns.tolist())
        cols = cols[cols != 'DATE']
        for n in range(1,lags+1):
            for i in cols:
                In_MEV['{}_lag{}'.format(i,n)] = In_MEV['{}'.format(i)].shift(n)

    return sqlContext.createDataFrame(In_MEV, )

print('2.2.1 Extracting and Testing Unemployment')
Unemp = Get_MEV(Unemp_link, transf = 'None', lags = 1)
print('2.2.2 Extracting and Testing GDP')
GDP = Get_MEV(GDP_link, transf = 'QoQ', lags = 1, mth_align=-1)
print('2.2.3 Extracting and Testing Inflation')
Infl = Get_MEV(Inflation_Link, transf = 'QoQ', lags = 1, mth_align=0)
print('2.2.4 Extracting and Testing House Price Index')
HPI = Get_MEV(HPI_Link, transf = 'QoQ', lags = 1, mth_align=0)
print('2.2.5 Extracting and Testing Disposable Income')
Disp_Inc = Get_MEV(Disp_Inc_Link, transf = 'QoQ', lags = 1, mth_align=0)
print('2.2.5 Extracting and Testing Employment')
Emp = Get_MEV(Employment_Link, transf = 'YoY', lags = 1, mth_align=0)

print('2.2.6 Joining all macroeconomic data together')
Out_MEV = Unemp
Out_MEV = Out_MEV.join(GDP, ['DATE'], 'outer')
Out_MEV = Out_MEV.join(Infl, ['DATE'], 'outer')
Out_MEV = Out_MEV.join(HPI, ['DATE'], 'outer')
Out_MEV = Out_MEV.join(Disp_Inc, ['DATE'], 'outer')
Out_MEV = Out_MEV.join(Emp, ['DATE'], 'outer')

print('2.2.6 Writing MEV data to Hive DB')
Out_MEV.createOrReplaceTempView("Out_MEV")
sqlContext.sql("""create database mev""")
sqlContext.sql("""create table if not exists mev.MEV_RAW as
            (select * from Out_MEV)""")


def Month_Diff(date1, date2):
    return ((Str_2_Dte(date2) - Str_2_Dte(date1))/np.timedelta64(1, 'M'))

Month_Diff_udf = udf(Month_Diff , TimestampType())
sqlContext.udf.register('Month_Diff_udf', Month_Diff_udf)


#Here is where we create the ABT
print('2.3.1 Creating ABT')
ABT = sqlContext.sql("""
            select period_start_dte
                ,   period_end_dte
                ,   bse.*
                
                ,   case when period_end_dte >= bse.Default_dte then 1
                    else 0
                    end as flow_to_df
                    
            from (
            select 
                last_day(period_end_dte) as period_end_dte
            ,   last_day(add_months(period_end_dte,-3)) as period_start_dte
            ,   id

            from (select distinct
            issue_dte as period_end_dte
            from work_db.inp_data
            where extract(month from issue_dte) in (3,6,9,12)
            ) as dt
            
            cross join (select distinct id from work_db.inp_data)
            
            ) as id
            
            inner join work_db.inp_data as bse on bse.id = id.id
            where bse.issue_dte <= id.period_end_dte
                and coalesce(bse.Default_dte, Maturity_Dte,cast('9999-12-31' as date)) > period_start_dte
            """)


#Checks and other shite
ABT = ABT.withColumn('TOB',floor(months_between(col('period_end_dte'),col('issue_dte'))/3))
print('2.3.2 Pushing ABT to Hive DB')
ABT.createOrReplaceTempView("ABT")
sqlContext.sql("""create table if not exists work_db.LC_DR_Periodic as
            (select * from ABT)""")

sc.stop()
