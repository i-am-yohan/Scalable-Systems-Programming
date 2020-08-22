#Test
from datetime import datetime
import findspark
findspark.init()
from pyspark.sql import SQLContext, Window, SparkSession
from pyspark import SparkConf, SparkContext, HiveContext
from pyspark.sql.functions import lit, row_number, monotonically_increasing_id, isnan, when, count, col, avg, udf
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType


conf = SparkConf().setMaster("yarn").set('spark.sql.warehouse.dir' , '/user/hive/warehouse')
sc = SparkContext(conf = conf)

sqlContext = HiveContext(sc)

print('1. Cleaning Data')
print('1.1 Importing Raw Data')
In_Df = sqlContext.read.csv('/Input/loan.csv' , header=True, inferSchema=True)

#Create the database for working with
sqlContext.sql("""create database work_db""")

#Add an ID variable
print('1.1 Creating ID variable')
In_Df = In_Df.withColumn('id', monotonically_increasing_id())
In_Df = In_Df.drop('member_id') #Drop Member ID because it is Blank

#Drop columns we dont need
print('1.2 Dropping unneccesary columns')
In_Df = In_Df.drop('url') #A cryptic column with only one value
In_Df = In_Df.drop('desc') # A Text column, not much can be extracted from this
In_Df = In_Df.drop('zip_code') #Unrelated to default rates

#Create binary default variable
print('1.3 Creating binary Default variable')
In_Df.createOrReplaceTempView("In_Df")
In_Df = sqlContext.sql("""select *
    ,   case when loan_status like '%Fully Paid%' then 0
            when loan_status like '%Current%' then 0
            when loan_status like 'Late (16-30 days)' then 0
            else 1
        end as Default_Ind
    from In_Df
    where loan_status != 'Oct-2015'
""")


print('1.4 Dropping unneccesary columns')
In_Df = In_Df.drop('debt_settlement')
In_Df = In_Df.drop('debt_settlement_flag')
In_Df = In_Df.drop('debt_settlement_flag_date')
In_Df = In_Df.drop('settlement_status')
In_Df = In_Df.drop('settlement_date')
In_Df = In_Df.drop('settlement_amount')
In_Df = In_Df.drop('settlement_percentage')
In_Df = In_Df.drop('settlement_term')

#Hardship Flag
In_Df.groupBy(['hardship_flag']).agg(F.count('id')).collect() #VerySparse - Lets remove
In_Df.groupBy(['hardship_flag','Default_Ind']).agg(F.count('id')).collect()

#Removing Hardship because it is sparse
In_Df = In_Df.drop('hardship_flag')
In_Df = In_Df.drop('hardship_type')
In_Df = In_Df.drop('hardship_reason')
In_Df = In_Df.drop('hardship_status')
In_Df = In_Df.drop('deferral_term')
In_Df = In_Df.drop('hardship_amount')
In_Df = In_Df.drop('hardship_start_date')
In_Df = In_Df.drop('hardship_end_date')
In_Df = In_Df.drop('payment_plan_start_date')
In_Df = In_Df.drop('hardship_length')
In_Df = In_Df.drop('hardship_dpd')
In_Df = In_Df.drop('hardship_loan_status')
In_Df = In_Df.drop('orig_projected_additional_accrued_interest')
In_Df = In_Df.drop('hardship_last_payment_amount')
In_Df = In_Df.drop('hardship_payoff_balance_amount')

In_Df = In_Df.drop('recoveries') #This is done after default

#Drop Secondary applicant Status
In_Df = In_Df.drop('sec_app_fico_range_low')
In_Df = In_Df.drop('sec_app_fico_range_high')
In_Df = In_Df.drop('sec_app_earliest_cr_line')
In_Df = In_Df.drop('sec_app_inq_last_6mths')
In_Df = In_Df.drop('sec_app_mort_acc')
In_Df = In_Df.drop('sec_app_open_acc')
In_Df = In_Df.drop('sec_app_revol_util')
In_Df = In_Df.drop('sec_app_open_act_il')
In_Df = In_Df.drop('sec_app_num_rev_accts')
In_Df = In_Df.drop('sec_app_chargeoff_within_12_mths')
In_Df = In_Df.drop('sec_app_collections_12_mths_ex_med')
In_Df = In_Df.drop('sec_app_mths_since_last_major_derog')

#Drop Joint application Variables
In_Df = In_Df.drop('annual_inc_joint')
In_Df = In_Df.drop('dti_joint')
In_Df = In_Df.drop('verification_status_joint')
In_Df = In_Df.drop('revol_bal_joint')

#Drop other variables that could be considered useless
In_Df = In_Df.drop('title') #Too Dense to extract anything from it

#These variables have an absurd number of missings - DROP!!
In_Df = In_Df.drop('open_acc_6m')
In_Df = In_Df.drop('open_act_il')
In_Df = In_Df.drop('open_il_12m')
In_Df = In_Df.drop('open_il_24m')
In_Df = In_Df.drop('mths_since_rcnt_il')
In_Df = In_Df.drop('total_bal_il')
In_Df = In_Df.drop('il_util')
In_Df = In_Df.drop('open_rv_12m')
In_Df = In_Df.drop('open_rv_24m')
In_Df = In_Df.drop('max_bal_bc')
In_Df = In_Df.drop('all_util')
In_Df = In_Df.drop('total_cu_tl')
In_Df = In_Df.drop('inq_last_12m')
In_Df = In_Df.drop('mths_since_recent_bc_dlq')
In_Df = In_Df.drop('mths_since_recent_revol_delinq')





#Check Null values
#In_Df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in In_Df.columns]).show()

#Change character values with missings
print('1.5 Imputing null values')
In_Df = In_Df.withColumn('annual_inc',In_Df['annual_inc'].cast('float'))
In_Df = In_Df.withColumn('total_bal_ex_mort',In_Df['total_bal_ex_mort'].cast('float'))
In_Df = In_Df.withColumn('loan_amnt',In_Df['loan_amnt'].cast('float'))
In_Df = In_Df.withColumn('dti',In_Df['dti'].cast('float'))
In_Df = In_Df.withColumn('last_pymnt_amnt',In_Df['last_pymnt_amnt'].cast('float'))
In_Df = In_Df.withColumn('open_acc',In_Df['open_acc'].cast('int'))
In_Df = In_Df.withColumn('revol_bal',In_Df['revol_bal'].cast('int'))


In_Df = In_Df.na.fill({'emp_title':'Not-Specified'
               ,'annual_inc':In_Df.approxQuantile('annual_inc' ,[0.5] , 0)[0]
               #,'title':'Not-Specified'
               ,'addr_state':'CA'#Its the biggest state...
               ,'last_pymnt_amnt':In_Df.approxQuantile('last_pymnt_amnt' ,[0.5] , 0)[0]
               ,'collection_recovery_fee':0
                ,'inq_last_6mths':0
                ,'delinq_2yrs':0
                ,'open_acc':In_Df.approxQuantile('open_acc' ,[0.5] , 0)[0]
                ,'pub_rec':0
                ,'revol_bal':In_Df.approxQuantile('revol_bal' ,[0.5] , 0)[0]
                ,'delinq_amnt':0
                ,'disbursement_method':'Cash'
                ,'dti':In_Df.approxQuantile('dti' ,[0.5] , 0)[0]
               })

#Impute
print('1.6 Cleaning up categorical columns')
In_Df.createOrReplaceTempView("In_Df")
In_Df = sqlContext.sql("""select *
    ,   coalesce(earliest_cr_line, issue_d) as earliest_cr_line_impute
    ,   case when purpose not in ('debt_consolidation'
                ,   'credit_card'
                ,   'home_improvement'
                ,   'other'
                ,   'major_purchase'
                ,   'medical'
                ,   'small_business'
                ,   'car'
                ,   'vacation'
                ,   'moving'
                ,   'house'
                ,   'wedding'
                ,   'renewable_energy'
                ,   'educational'
                ) then 'other'
            else purpose
        end as purpose_clean
        
    ,   case when disbursement_method = 'N' then 'Cash'
        else disbursement_method
        end as disbursement_method_clean
        
    ,   case when tot_coll_amt is null or tot_coll_amt = 0 then 0
        else 1
        end as Secured_Ind
        
            
    from In_Df
""")


In_Df = In_Df.drop('earliest_cr_line')
In_Df = In_Df.drop('next_pymnt_d')
In_Df = In_Df.drop('purpose')
In_Df = In_Df.drop('disbursement_method')

#Create function to change date
def Str_2_Dte(x):
    return(datetime.strptime(x , '%d-%b-%Y'))

Str_2_Dte_udf = udf(Str_2_Dte , TimestampType())
sqlContext.udf.register('SQL_Str_2_Dte_udf', Str_2_Dte_udf)


#Only Keep "important" variables - varibales that might identify a default
#We are using last payment date to derive the default date - so remove all invalid ones
print('1.7 Keeping variables required for analysis')
In_Df.createOrReplaceTempView("In_Df")
In_Df = sqlContext.sql("""select
                        id
                    ,   loan_amnt
                    ,   term
                    ,   int_rate
                    ,   installment
                    ,   grade
                    ,   sub_grade
                    ,   emp_title
                    ,   emp_length
                    ,   home_ownership
                    ,   annual_inc
                    ,   dti
                    ,   tot_coll_amt
                    ,   purpose_clean
                    ,   Secured_Ind
                    ,   delinq_amnt
                    ,   cast(SQL_Str_2_Dte_udf(upper(concat('01-',last_pymnt_d))) as Date) as last_pymnt_dte
                    ,   cast(SQL_Str_2_Dte_udf(upper(concat('01-',issue_d))) as Date) as issue_dte
                    ,   Default_Ind
                    ,   loan_status
                    from In_Df
                    where last_pymnt_d is not null 
                        and last_pymnt_d like '%-20%'
                        """)


In_Df.createOrReplaceTempView("In_Df")
In_Df = sqlContext.sql("""select
                        *
                    ,   Case when Default_Ind = 1 then Add_months(last_pymnt_dte, 1)
                        end as Default_dte
                    ,   Case when loan_status like '%Fully Paid%' then last_pymnt_dte
                        end as Maturity_Dte
                    
                    from In_Df
                    """)

In_Df = In_Df.drop('loan_status')

print('1.8 Writing Cleaned table to Hive DB')
In_Df.createOrReplaceTempView("In_Df")
sqlContext.sql('''create table work_db.Inp_Data as 
                (select * from In_Df)''')
#In_Df.coalesce(1).write.mode('overwrite').csv('hdfs://localhost:9000/SSP/Project/01_Cleaned_Data' , header=True)

#Load to Hive Database
sc.stop()
