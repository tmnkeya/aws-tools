NUM_SAMPLES=400

run_bc()
{
    echo "Batch Common Attributes"
    python tsdb_manager.py
    echo ""
    python ts_cont_ingestor.py --ingestion-mode batchCommon  -n $NUM_SAMPLES -c 16 | grep Total
    echo ""
}

run_b()
{

    echo "Batch"
    python tsdb_manager.py
    echo ""
    python ts_cont_ingestor.py --ingestion-mode batch  -n $NUM_SAMPLES -c 16 | grep Total
    echo ""
}

run_s()
{
    echo "Single"
    python tsdb_manager.py
    echo ""
    python ts_cont_ingestor.py --ingestion-mode single -n $NUM_SAMPLES -c 16 | grep Total
    echo ""
}


run_bc
run_b

# run_s
