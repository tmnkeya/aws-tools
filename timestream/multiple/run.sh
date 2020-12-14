NUM_SAMPLES=10000
CONC=16

run_bc()
{
    echo "Testing batch common attributes mode ..."
    python tsdb_manager.py
    # sleep 3
    echo ""
    python ts_cont_ingestor.py --ingestion-mode batchCommon  -n $NUM_SAMPLES -c $CONC 
    echo ""
}

run_b()
{

    echo "Testing batch mode .... "
    python tsdb_manager.py
    # sleep 3
    echo ""
    python ts_cont_ingestor.py --ingestion-mode batch  -n $NUM_SAMPLES -c $CONC 
    echo ""
}

run_s()
{
    echo "Single"
    python tsdb_manager.py
    sleep 3
    echo ""
    python ts_cont_ingestor.py --ingestion-mode single -n $NUM_SAMPLES -c $CONC # | grep Total
    echo ""
}
run_loop()
{
    
    for num_samples in 1000; do
	for conc in 4; do
	    for mode in batchCommon batch; do
		echo "-----"
		echo $mode $num_samples $conc
		python tsdb_manager.py # > /dev/null 2>&1
		sleep 5
		echo
		python ts_cont_ingestor.py --ingestion-mode $mode  -n $num_samples -c $conc # | grep -E "Total|Samples Cnt"
		echo
	    done
	    echo
	done
    done

}


run_bc
# run_b
# run_s
# run_loop;
