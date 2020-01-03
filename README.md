# 基於LDA機器學習演算法之雲端自動分類儲存系統
DownloadTempFile資料夾用來暫時存放要分析的文章(會自動從hdfs下載)
<br>
filereaderAPI.py用來讀取不同副檔名的文章內容(word、pdf、txt)
<br>
hdfsAPI.py用來查詢以及下載hdfs的檔案
<br>
hlda.py為主程式利用以下指令執行(路徑以及virtualenv請自行更改)
<br>
/home/hadoop/spark-2.4.3-bin-hadoop2.7/bin/spark-submit --master yarn --conf spark.pyspark.virtualenv.enabled
=true --conf spark.pyspark.virtualenv.requirements=/home/hadoop/requirements.txt --conf spark.pyspark.virtualenv.bin.path=/home/hadoop/env1 --conf spark.pyspark.python=/home/hadoop/env1/bin/python3 /home/hadoop/finalproject/hlda.py <分群數量>
