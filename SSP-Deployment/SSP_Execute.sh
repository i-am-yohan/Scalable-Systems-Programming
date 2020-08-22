#1. Get Data
hdfs dfs -mkdir /Input
wget https://ssp-x19163568.s3.amazonaws.com/loan.csv
hdfs dfs -copyFromLocal loan.csv /Input
rm loan.csv

#2. Execute DB Analysis
hdfs dfs -mkdir /In_Model
sudo pip3 install -r Requirements.txt
python3 Scripts/01_Clean_Up.py #1. Clean data and Upload
python3 Scripts/02_Periodic_tbl.py #2. Create Periodic table and load MEVs
python3 Scripts/03_Visual.py #3. Create input to Tableau - insights derived here
python3 Scripts/04_Create_Train.py #4. Create data for modelling

#3. Modelling
mkdir Visual
Rscript Scripts/05_Modelling.R

