# employee-activity-etl-poc

.\venv\Scripts\activate.bat




PS C:\Users\Dev\Desktop\OpenClassrooms\Project12\employee-activity-etl-poc\spark_consumer> curl.exe http://localhost:8083/connectors/postgres-connector/status | ConvertFrom-Json
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   174  100   174    0     0   4815      0 --:--:-- --:--:-- --:--:--  4833

name               connector                                   tasks                                               type
----               ---------                                   -----                                               ----
postgres-connector @{state=RUNNING; worker_id=172.18.0.3:8083} {@{id=0; state=RUNNING; worker_id=172.18.0.3:8083}} source


docker exec redpanda rpk topic list

NAME                               PARTITIONS  REPLICAS
connect_configs                    1           1
connect_offsets                    25          1
connect_statuses                   5           1
pg_cdc.public.employee_activities  1           1
PS C:\Users\Dev\Desktop\OpenClassrooms\Project12\employee-activity-etl-poc>





delete redpanda topic (debezium recreates it)

docker exec -it redpanda /bin/bash

rpk topic list
rpk topic delete pg_cdc.public.employee_activities


in pgAdmin


delete last 7 entries :

DELETE FROM employee_activities 
WHERE "ID" IN (
  SELECT "ID" 
  FROM employee_activities 
  ORDER BY "ID" DESC 
  LIMIT 7
);


delete all

DELETE FROM employee_activities;