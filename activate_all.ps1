# Start in new wt tab
wt --title base
Start-Process wt "-w 0 focus-tab -t 0 nt --title zookeeper1 powershell zookeeper-server-start C:\Users\Roy\apache\apache-zookeeper-3.7.0-bin\conf\zoo.cfg"
Start-Process wt "-w 0 focus-tab -t 0 nt --title zookeeper1 powershell zookeeper-server-start C:\Users\Roy\apache\apache-zookeeper-3.7.0-bin2\conf\zoo.cfg"
Start-Sleep 10
Start-Process wt "-w 0 focus-tab -t 0 nt --title kafka1 powershell kafka-server-start C:\Users\Roy\kafka\config\server1.properties"
Start-Process wt "-w 0 focus-tab -t 0 nt --title kafka2 powershell kafka-server-start C:\Users\Roy\kafka\config\server2.properties"
Write-Output "up"


# stop kafka and zookepper
#kafka-server-stop --bootstrap-server localhost:9092,localhost:9093
#zookeeper-server-stop
#Write-Output "down"
