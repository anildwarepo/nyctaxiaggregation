#/bin/bash

mvn clean package -Dmaven.test.skip=true
azcopy copy "/mnt/c/Users/anildwa/source/repos/nyctaxidataspark/nyctaxidata/target/nyctaxidata-1.0.jar" "https://anildwaadlsv2.dfs.core.windows.net/jars/nyctaxidata-1.0.jar"
azcopy copy "https://anildwaadlsv2.blob.core.windows.net/nytaxidata/metrics/metrics.txt" "/mnt/c/Users/anildwa/Downloads/metrics.txt"
cat /mnt/c/Users/anildwa/Downloads/metrics.txt