import findspark
findspark.init()
from pyspark.sql import SQLContext, HiveContext
from pyspark import SparkConf, SparkContext
import pandas as pd
import numpy as np

conf = SparkConf().setMaster("local").set('spark.sql.warehouse.dir' , 'hdfs://localhost:9000/user/hive/warehouse')
sc = SparkContext(conf = conf)

sqlContext = HiveContext(sc)

#Create training Table
#sqlContext.sql("""create database if not exists model""")
ABT_Train = sqlContext.sql("""
--create table if not exists model.ABT_Train as (
select  id
    ,   period_end_dte as period_dte
    ,   grade
    ,   purpose_clean
    ,   TOB
    ,   UNRATE
    ,   DSPIC96_QoQ
    ,   flow_to_df

from work_db.lc_dr_periodic as bse
left join mev.mev_raw as mev on bse.period_end_dte = mev.DATE
--)
""")


#Load ABT Forecasts
#hardcoded from EBA Doc
Forecasts = pd.DataFrame(data = {'Scenario':['BASE','BASE','BASE','BASE','ADVERSE','ADVERSE','ADVERSE','ADVERSE'],
                            'Year':[2019,2020,2021,2022,2019,2020,2021,2022],
                          'Unemp':[3.7,3.5,3.6,3.6,3.7,5.2,6.8,7.8],
                          'GDP':[0.024,0.021,0.017,0.017,0.024,-0.021,-0.027,-0.01]
                          })

Forecasts_SP = sqlContext.createDataFrame(Forecasts)
Forecasts_SP.createOrReplaceTempView("Forecasts_SP")

#Create table of predictions
Fcst_Dim = pd.DataFrame(data = {'period_end_dte':['2019-12-31','2020-03-31','2020-03-31','2020-06-30','2020-12-31'
                                                    ,'2021-03-31','2021-03-31','2021-06-30','2021-12-31'
                                                    ,'2022-03-31','2022-03-31','2022-06-30','2022-12-31'
                                                   ],
                                 'Min_TOB':[0,1,2,3,4,5,6,7,8,9,10,11,12]
                                 } ,
                        #dtype=np.dtype([('period_end_dte',np.datetime64),('Min_TOB',int)])
                        )
Fcst_Dim['period_end_dte'] = Fcst_Dim['period_end_dte'].astype('datetime64[ns]')
Fcst_Dim = sqlContext.createDataFrame(Fcst_Dim)
Fcst_Dim.createOrReplaceTempView("Fcst_Dim")


ABT_Predict = sqlContext.sql("""
--create table if not exists model.ABT_Predict as (
select Min_TOB
    ,   Min_TOB + pred.TOB as TOB
    ,   cast(last_day(add_months(cast('2019-12-31' as date),pred.TOB*3)) as date) as period_dte
    ,   grade
    ,   purpose_clean
    ,   Scenario
    ,   Unemp as UNRATE
    ,   cast((POWER(1+GDP, 1/4) - 1) as decimal(12,8)) as DSPIC96_QoQ
    ,   TOB_ODR
    --,   (1 + GDP)^(1/4) - 1 as DSPIC96_QoQ

from Fcst_Dim as bse
cross join (select distinct
            grade
        ,   purpose_clean
        ,   TOB
       from work_db.lc_dr_periodic) as pred
        
inner join Forecasts_SP as fc on extract(year from last_day(add_months(cast('2019-12-31' as date),pred.TOB*3))) = fc.Year
left join (select TOB
            ,   sum(flow_to_df)/count(*) as TOB_ODR
            from work_db.lc_dr_periodic
            group by 1) as odr on odr.TOB = Min_TOB + pred.TOB

where pred.TOB >= Min_TOB
    and pred.TOB <= (select max(TOB) from work_db.lc_dr_periodic)
--)
""")



#Create ARIMA Table
ARIMA_Train = sqlContext.sql("""
--create table if not exists model.ARIMA_TRAIN as (
select  period_end_dte
    ,   grade
    ,   UNRATE
    ,   UNRATE_lag1
    ,   GDPC1_QoQ
    ,   GDPC1_QoQ_lag1
    ,   DSPIC96_QoQ
    ,   DSPIC96_QoQ_lag1
    ,   CPIAUCSL_QoQ
    ,   CPIAUCSL_QoQ_lag1
    ,   CSUSHPISA_QoQ
    ,   CSUSHPISA_QoQ_lag1
    ,   PAYEMS_YoY
    ,   PAYEMS_YoY_lag1
    ,   ODR

from (
    select
        period_end_dte
    ,   grade
    ,   sum(flow_to_df)/count(*) as ODR
    from work_db.lc_dr_periodic
    group by 1,2
    ) as bse
left join mev.mev_raw as mev on bse.period_end_dte = mev.DATE
--)
""")

#Redundant
ARIMA_Predict = sqlContext.sql("""
--create table if not exists model.ARIMA_Predict as (
select cast(period_end_dte as date) as period_end_dte
    ,   Scenario
    ,   Unemp as UNRATE
    ,   cast((POWER(1+GDP, 1/4) - 1) as decimal(12,8)) as DSPIC96_QoQ
from (select distinct period_end_dte
    from Fcst_Dim) as bse
    
left join Forecasts_SP as fc on extract(year from period_end_dte) = fc.Year
--    )
""")

ABT_Train.coalesce(1).write.mode('overwrite').csv('hdfs://localhost:9000/In_Model/ABT_Train' , header=True)
ARIMA_Train.coalesce(1).write.mode('overwrite').csv('hdfs://localhost:9000/In_Model/ARIMA_Train' , header=True)
ABT_Predict.coalesce(1).write.mode('overwrite').csv('hdfs://localhost:9000/In_Model/ABT_Predict' , header=True)
ARIMA_Predict.coalesce(1).write.mode('overwrite').csv('hdfs://localhost:9000/In_Model/ARIMA_Predict' , header=True)

sc.stop()
