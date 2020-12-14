# python manage.py


# -g False by default

# python ingest.py --database-name TestDB1 --table-name BatchTbl1 --endpoint us-west-2  &
# sleep 60
# sudo pkill -9 -f python
# sleep 5

# python ingest.py --database-name TestDB1 --table-name BatchTbl1 --endpoint us-west-2 -b 1 & 
# sleep 60
# sudo pkill -9 -f python
# sleep 5

# python ingest.py --database-name TestDB1 --table-name BatchTbl1 --endpoint us-west-2 -b 19 &
# sleep 60
# sudo pkill -9 -f python
# sleep 5

# python ingest.py --database-name TestDB1 --table-name BatchTbl1 --endpoint us-west-2 -b 20 &
# sleep 60
# sudo pkill -9 -f python
# sleep 5

# python ingest.py --database-name TestDB1 --table-name BatchTbl1 --endpoint us-west-2 -b 21 & 
# sleep 60
# sudo pkill -9 -f python
# sleep 5

# python ingest.py --database-name TestDB1 --table-name BatchTbl1 --endpoint us-west-2 -b 100 & 
# sleep 60
# sudo pkill -9 -f python
# sleep 5


# Group by measure name and packed into a single batch for write_records() call.
python ingest.py --database-name TestDB1 --table-name BatchTbl1 --endpoint us-west-2 -b 1   -g True & 
sleep 60
sudo pkill -9 -f python
sleep 5

python ingest.py --database-name TestDB1 --table-name BatchTbl1 --endpoint us-west-2 -b 19  -g True & 
sleep 60
sudo pkill -9 -f python
sleep 5

python ingest.py --database-name TestDB1 --table-name BatchTbl1 --endpoint us-west-2 -b 20  -g True & 
sleep 60
sudo pkill -9 -f python
sleep 5

python ingest.py --database-name TestDB1 --table-name BatchTbl1 --endpoint us-west-2 -b 21  -g True & 
sleep 60
sudo pkill -9 -f python
sleep 5

python ingest.py --database-name TestDB1 --table-name BatchTbl1 --endpoint us-west-2 -b 100  -g True & 
sleep 60
sudo pkill -9 -f python
sleep 5


# python ingest.py --database-name TestDB1 --table-name BatchTbl1 --endpoint us-west-2 -b 101 & 
# sleep 5
# sudo pkill -9 -f python
# sleep 5

# python ingest.py --database-name TestDB1 --table-name BatchTbl1 --endpoint us-west-2 -b 101  -g True & 
# sleep 5
# sudo pkill -9 -f python
