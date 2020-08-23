# Scalable-Systems-Programming
All of the necessary code can be got by calling the following command:
```
wget https://github.com/i-am-yohan/Scalable-Systems-Programming/archive/master.zip
```
Once the file is unzipped enter the directory with the ```SSP_Execute.sh``` bash script inside.
Call the script as follows (might vary depending on the distribution of linux):
```
SSP_Execute.sh
```
Ensure that the script is called from the same directory as it is in on the downloaded zip file.

The follwoing must be ensured before deployment:
- Spark, Hive, Hadoop and all necessary dependancies are installed.
- R can install packages such as H2o easily.

The tableau visualization is saved as follows:
https://ssp-x19163568.s3.amazonaws.com/SSP+Visualization+Submission.twbx

The source data is saved on S3 because Lending CLub now require membership to access their dataset. This must have only came into effect sometime during the research.
