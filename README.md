First Lambda Name:Lambda_B
Desription:
Main work of Lambda_B to download meta data from sources and store it in aws s3 bucket.

Second Lambda Name:cwatr
Desription:
Second lambda use to download data  againt meta data of lambda one and stored it s3 and stort it at RDS


Note:
First lambda invoke name Lambda_B download meta data and after download the data ,first lambda will invoke to second lambda name:cwtar.
