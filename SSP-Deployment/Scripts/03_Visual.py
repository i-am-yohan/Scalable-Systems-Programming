#Data prep for tableau
import findspark
findspark.init()
from pyspark.sql import SQLContext, HiveContext
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("yarn").set('spark.sql.warehouse.dir' , 'hdfs://localhost:9000/user/hive/warehouse')
sc = SparkContext(conf = conf)

sqlContext = HiveContext(sc)


#the TOB Binning
def ODR_STR(Group, Time_Dim):
    out = sqlContext.sql("""
            select {tdim}
                ,   '{bin}' as Feature
                ,   {bin} as Bin
                ,   sum(flow_to_df)/count(*) as ODR
                ,   sum(loan_amnt) as Exposure
                ,   count(*) as number_of_loans
            from work_db.LC_DR_Periodic
            group by 1,2,3
               """.format(tdim = Time_Dim, bin = Group))
    return out

def ODR_NUM(Group, Time_Dim):
    out = sqlContext.sql("""
            select {tdim}
                ,   '{bin}' as Feature
                ,   {bin}_Bin as Bin
                ,   sum(flow_to_df)/count(*) as ODR
                ,   sum(loan_amnt) as Exposure
                ,   count(*) as number_of_loans
            from (select *
                    ,   concat('{bin}_Decile_' , ntile(10) over(PARTITION BY {bin}, {tdim} ORDER BY {bin})) as {bin}_Bin
                    from work_db.LC_DR_Periodic
                    )
            group by 1,2,3
               """.format(tdim = Time_Dim, bin = Group))
    return out


#The TOB segment
TOB_Vis = ODR_STR('term', 'TOB')
TOB_Vis = TOB_Vis.union(ODR_NUM('int_rate', 'TOB'))
TOB_Vis = TOB_Vis.union(ODR_STR('grade', 'TOB'))
TOB_Vis = TOB_Vis.union(ODR_STR('sub_grade', 'TOB'))
TOB_Vis = TOB_Vis.union(ODR_STR('emp_title', 'TOB'))
TOB_Vis = TOB_Vis.union(ODR_STR('emp_length', 'TOB'))
TOB_Vis = TOB_Vis.union(ODR_STR('home_ownership', 'TOB'))
TOB_Vis = TOB_Vis.union(ODR_NUM('annual_inc', 'TOB'))
TOB_Vis = TOB_Vis.union(ODR_NUM('dti', 'TOB'))
TOB_Vis = TOB_Vis.union(ODR_NUM('tot_coll_amt', 'TOB'))
TOB_Vis = TOB_Vis.union(ODR_STR('purpose_clean', 'TOB'))
TOB_Vis = TOB_Vis.union(ODR_STR('secured_ind', 'TOB'))
TOB_Vis = TOB_Vis.union(ODR_NUM('delinq_amnt', 'TOB'))


Date_Vis = ODR_STR('term', 'period_end_dte')
Date_Vis = Date_Vis.union(ODR_NUM('int_rate', 'period_end_dte'))
Date_Vis = Date_Vis.union(ODR_STR('grade', 'period_end_dte'))
Date_Vis = Date_Vis.union(ODR_STR('sub_grade', 'period_end_dte'))
Date_Vis = Date_Vis.union(ODR_STR('emp_title', 'period_end_dte'))
Date_Vis = Date_Vis.union(ODR_STR('emp_length', 'period_end_dte'))
Date_Vis = Date_Vis.union(ODR_STR('home_ownership', 'period_end_dte'))
Date_Vis = Date_Vis.union(ODR_NUM('annual_inc', 'period_end_dte'))
Date_Vis = Date_Vis.union(ODR_NUM('dti', 'period_end_dte'))
Date_Vis = Date_Vis.union(ODR_NUM('tot_coll_amt', 'period_end_dte'))
Date_Vis = Date_Vis.union(ODR_STR('purpose_clean', 'period_end_dte'))
Date_Vis = Date_Vis.union(ODR_STR('secured_ind', 'period_end_dte'))
Date_Vis = Date_Vis.union(ODR_NUM('delinq_amnt', 'period_end_dte'))

sqlContext.sql("""create database if not exists model""")
Date_Vis.createOrReplaceTempView("Date_Vis")
sqlContext.sql("""create table model.Dte_Vis as (select * from Date_Vis)""")
TOB_Vis.createOrReplaceTempView("TOB_Vis")
sqlContext.sql("""create table model.TOB_Vis as (select * from TOB_Vis)""")
sc.stop()
