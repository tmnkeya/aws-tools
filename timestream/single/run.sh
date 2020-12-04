SLP=10

for B in 100; do
    for N in 100 1000; do
    #for N in 100 1000 10000 100000; do
	python ingestion.py -b $B -n $N -c False
	# sleep $SLP
	python ingestion.py -b $B -n $N -c True
	# sleep $SLP
    done
done


run_each()
{
python ingestion.py -b 10 -n 10 -c False
sleep $SLP
python ingestion.py -b 10 -n 10 -c True
sleep $SLP

python ingestion.py -b 10 -n 100 -c False
sleep $SLP
python ingestion.py -b 10 -n 100 -c True
sleep $SLP

python ingestion.py -b 10 -n 1000 -c False
sleep $SLP
python ingestion.py -b 10 -n 1000 -c True
sleep $SLP

python ingestion.py -b 10 -n 10000 -c False
sleep $SLP
python ingestion.py -b 10 -n 10000 -c True
sleep $SLP

python ingestion.py -b 100 -n 100 -c False
sleep $SLP
python ingestion.py -b 100 -n 100 -c True
sleep $SLP

python ingestion.py -b 100 -n 1000 -c False
sleep $SLP

python ingestion.py -b 100 -n 1000 -c True
sleep $SLP

python ingestion.py -b 100 -n 10000 -c False
sleep $SLP
python ingestion.py -b 100 -n 10000 -c True
sleep $SLP

python ingestion.py -b 100 -n 100000 -c False
sleep $SLP
python ingestion.py -b 100 -n 100000 -c True
sleep $SLP
}
