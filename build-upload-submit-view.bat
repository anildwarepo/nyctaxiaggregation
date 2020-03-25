mvn clean package -Dmaven.test.skip=true
c:\azcopy\azcopy copy "C:\Users\anildwa\source\repos\nyctaxidataspark\nyctaxidata\target\nyctaxidata-1.0.jar" "https://anildwaadlsv2.dfs.core.windows.net/jars/nyctaxidata-1.0.jar"

REM c:\azcopy\azcopy copy "https://anildwaadlsv2.blob.core.windows.net/nytaxidata/user%2F185%2Fmetrics.txt" "C:\Users\anildwa\Downloads"\metrics.txt"