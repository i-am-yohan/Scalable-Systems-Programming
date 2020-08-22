list.of.packages <- c('sparklyr','reshape2','forecast','LaplacesDemon','lubridate')
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) {
  install.packages(new.packages, repos='http://cran.us.r-project.org')
  #spark_install()
}

install.packages("h2o", repos=(c("http://s3.amazonaws.com/h2o-release/h2o/master/1497/R", getOption("repos"))))

library('h2o')
library('sparklyr')
library('reshape2')
library('forecast')
library('LaplacesDemon')
library('lubridate')


#Cox-PH Model
h2o.init()

ABT_Train <- h2o.importFile('hdfs://localhost:9000/In_Model/ABT_Train')
ABT_Predict <- h2o.importFile('hdfs://localhost:9000/In_Model/ABT_Predict',skipped_columns = 3)


Model <- h2o.coxph(
  x = c('grade','purpose_clean','UNRATE','DSPIC96_QoQ')
  ,event_column = 'flow_to_df'
  ,stop_column = 'TOB'
  ,training_frame = ABT_Train
)

out_predict <- ABT_Predict
out_predict['PD'] <- invlogit(logit(ABT_Predict['TOB_ODR'])*exp(-h2o.predict(Model , ABT_Predict)))
h2o.exportFile(out_predict , paste(c(getwd(),'/Visual/Cox_PH_Predict.csv'),collapse=''), force=TRUE)

h2o.shutdown(prompt = FALSE)



#ARIMA Stuff
sc <- spark_connect(master='local') #Using Spark to get files from hdfs
ARIMA_Train <- spark_read_csv(sc,'hdfs://localhost:9000/In_Model/ARIMA_Train')
ARIMA_Predict <- spark_read_csv(sc,'hdfs://localhost:9000/In_Model/ARIMA_Predict')
ARIMA_Train <- as.data.frame(ARIMA_Train)
ARIMA_Predict <- as.data.frame(ARIMA_Predict)

#ARIMA_Train <- read.csv('/home/hduser/SSP/Project/Development/05_Modelling/data/ARIMA_Train.csv') #temp
#ARIMA_Predict <- read.csv('/home/hduser/SSP/Project/Development/05_Modelling/data/ARIMA_Predict.csv') #temp

mev_list <- c('UNRATE'
  ,'UNRATE_lag1'
  ,'GDPC1_QoQ'
  ,'GDPC1_QoQ_lag1'
  ,'DSPIC96_QoQ'
  ,'DSPIC96_QoQ_lag1'
  ,'CPIAUCSL_QoQ'
  ,'CPIAUCSL_QoQ_lag1'
  ,'CSUSHPISA_QoQ'
  ,'CSUSHPISA_QoQ_lag1'
  ,'PAYEMS_YoY'
  ,'PAYEMS_YoY_lag1')

ARIMA_Train['ODR'] <- qnorm(ARIMA_Train[,'ODR'])
ARIMA_Train_t <- melt(ARIMA_Train
                      , c('period_end_dte','grade',mev_list)
                      , 'ODR'
                      )

f <- paste(paste(c('period_end_dte',mev_list) , collapse = '+'),'grade',sep='~')
ARIMA_Train_t <- dcast(ARIMA_Train, f)
ARIMA_Train_t['period_end_dte'] <- as.Date(ARIMA_Train_t[,'period_end_dte'])
ARIMA_Train_ABT <- ARIMA_Train_t[as.numeric(format(ARIMA_Train_t[,'period_end_dte'],'%Y')) >= 2009,]
ARIMA_Train_ABT <- ARIMA_Train_ABT[order(ARIMA_Train_ABT['period_end_dte']),]


Model_A <- auto.arima(ARIMA_Train_ABT[-1,'A'])
print(Model_A)
print('P-values')
print((1-pnorm(abs(Model_A$coef)/sqrt(diag(Model_A$var.coef))))*2)

Model_B <- auto.arima(ARIMA_Train_ABT['B'])
print(Model_B)
print('P-values')
print((1-pnorm(abs(Model_B$coef)/sqrt(diag(Model_B$var.coef))))*2)

Model_C <- auto.arima(ARIMA_Train_ABT['C'])
print(Model_C)
print('P-values')
(1-pnorm(abs(Model_C$coef)/sqrt(diag(Model_C$var.coef))))*2

Model_D <- auto.arima(ARIMA_Train_ABT['D'])
print(Model_D)
print('P-values')
(1-pnorm(abs(Model_D$coef)/sqrt(diag(Model_D$var.coef))))*2

Model_E <- auto.arima(ARIMA_Train_ABT['E'])
print(Model_E)
print('P-values')
(1-pnorm(abs(Model_E$coef)/sqrt(diag(Model_E$var.coef))))*2

Model_F <- auto.arima(ARIMA_Train_ABT['F'])
print(Model_F)
print('P-values')
(1-pnorm(abs(Model_F$coef)/sqrt(diag(Model_F$var.coef))))*2

Model_G <- auto.arima(ARIMA_Train_ABT['G'])
print(Model_G)
print('P-values')
(1-pnorm(abs(Model_G$coef)/sqrt(diag(Model_G$var.coef))))*2


#Forecasts
#Create date forecasts
date_df <- data.frame('date' = c('2019-03-31','2019-06-30','2019-09-30','2019-12-31',
                                 '2020-03-31','2020-06-30','2020-09-30','2020-12-31',
                                 '2021-03-31','2021-06-30','2021-09-30','2021-12-31',
                                 '2022-03-31','2022-06-30','2022-09-30','2022-12-31'
                                 ))

date_df <- as.Date(date_df[,'date'])

A_fcst <- cbind(date_df,data.frame('grade'='A'),data.frame(forecast(Model_A, h=16)))
B_fcst <- cbind(date_df,data.frame('grade'='B'),data.frame(forecast(Model_B, h=16)))
C_fcst <- cbind(date_df,data.frame('grade'='C'),data.frame(forecast(Model_C, h=16)))
D_fcst <- cbind(date_df,data.frame('grade'='D'),data.frame(forecast(Model_D, h=16)))
E_fcst <- cbind(date_df,data.frame('grade'='E'),data.frame(forecast(Model_E, h=16)))
F_fcst <- cbind(date_df,data.frame('grade'='F'),data.frame(forecast(Model_F, h=16)))
G_fcst <- cbind(date_df,data.frame('grade'='G'),data.frame(forecast(Model_G, h=16)))

ARIMA_Forecast <- rbind(A_fcst,B_fcst,C_fcst,D_fcst,E_fcst,F_fcst,G_fcst)
ARIMA_Forecast['Point.Forecast'] <- pnorm(ARIMA_Forecast[,'Point.Forecast'])
ARIMA_Forecast['Lo.80'] <- pnorm(ARIMA_Forecast[,'Lo.80'])
ARIMA_Forecast['Hi.80'] <- pnorm(ARIMA_Forecast[,'Hi.80'])
ARIMA_Forecast['Lo.95'] <- pnorm(ARIMA_Forecast[,'Lo.95'])
ARIMA_Forecast['Hi.95'] <- pnorm(ARIMA_Forecast[,'Hi.95'])
write.csv(ARIMA_Forecast, paste(c(getwd(),'/Visual/ARIMA_Predict.csv'),collapse=''))

