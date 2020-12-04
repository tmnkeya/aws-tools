SELECT hostname,
       bin(time, 5m) as binned_time,
       avg(measure_value::double) as avg_cpu_utilization
  FROM TestDB.BatchTbl
 WHERE measure_name = 'cpu_utilization'
   AND time > ago(2h)
 GROUP BY hostname, bin(time, 5m)
 
