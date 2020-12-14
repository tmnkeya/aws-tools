# python manage.py
# sleep 5

# Single thread, batch 1, 100, 101, no grouping by measurename
# python ingest.py --database-name TestDB1 --table-name BatchTbl1 --endpoint us-west-2 -c 1 -b 1 & 
# MY_PID=$!
# sleep 10
# sudo kill -SIGINT $MY_PID
# sleep 5

python ingest.py --database-name TestDB1 --table-name BatchTbl1 --endpoint us-west-2 -c 1 -b 100 & 
MY_PID=$!
sleep 60
sudo kill -SIGINT $MY_PID
sleep 5

# python ingest.py --database-name TestDB1 --table-name BatchTbl1 --endpoint us-west-2 -c 1 -b 101 & 
# MY_PID=$!
# sleep 3
# sudo kill -SIGINT $MY_PID
# sleep 5


# Single thread, batch 1, 100, 101, grouping by measurename

# python ingest.py --database-name TestDB1 --table-name BatchTbl1 --endpoint us-west-2 -c 1 -b 1 -g True & 
# MY_PID=$!
# sleep 10
# sudo kill -SIGINT $MY_PID
# sleep 5

# python ingest.py --database-name TestDB1 --table-name BatchTbl1 --endpoint us-west-2 -c 1 -b 100 -g True & 
# MY_PID=$!
# sleep 60
# sudo kill -SIGINT $MY_PID
# sleep 5

python ingest.py --database-name TestDB1 --table-name BatchTbl1 --endpoint us-west-2 -c 1 -b 101 -g True & 
MY_PID=$!
sleep 3
sudo kill -SIGINT $MY_PID
sleep 3


