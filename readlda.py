#!/usr/bin/env python
#coding=utf-8
from pyspark.ml.clustering import LDA,LocalLDAModel
import jieba
import os
from pyspark.sql import SparkSession
from pyspark import SparkContext
import os
from pyspark.ml.feature import CountVectorizer,CountVectorizerModel
from hdfs.client import Client
import hdfsAPI as hdfs 
import filereaderAPI as reader
#preprocessing
user="001"
jieba.load_userdict('/home/hadoop/dict.txt.big') 

filepath=hdfs.list_files("/User/"+user+"/Data")

#print(filepath)
for i in filepath:
        ftype=os.path.splitext(i)[-1]
        if ftype==".txt" or ftype==".pdf" or ftype==".docx":
                hdfs.download_file(i,"/home/hadoop/finalproject/DownloadTempFile")

texts=[]
count=0
for i in filepath:
        texts.append([])
        filename=i.split("/")[-1]
        ftype=os.path.splitext(i)[-1]
        s=""
        if ftype=='.txt':
                s=reader.readtxt("/home/hadoop/finalproject/DownloadTempFile/"+filename)
                os.remove("/home/hadoop/finalproject/DownloadTempFile/"+filename)
        if ftype=='.pdf':
                s=reader.readpdf("/home/hadoop/finalproject/DownloadTempFile/"+filename)
                os.remove("/home/hadoop/finalproject/DownloadTempFile/"+filename)
        if ftype=='.docx':
                s=reader.readdocx("/home/hadoop/finalproject/DownloadTempFile/"+filename)
                os.remove("/home/hadoop/finalproject/DownloadTempFile/"+filename)
        s=s.replace('\n','')
        s=jieba.cut(s,cut_all=False)
        f=open("/home/hadoop/stopword.txt","r",encoding='utf8')
        stopwords=f.read()
        stopwords=stopwords.split('\n')
        wordlist=[]
        for word in s:
                if word not in stopwords and word!='\r':
                        wordlist.append(word)
        texts[len(texts)-1].append(count)
        texts[len(texts)-1].append(wordlist)
        count+=1


spark=SparkSession.builder.appName("dataFrame").getOrCreate()
df = spark.createDataFrame(texts, ["id", "words"])

# sc =SparkContext()
# dataset = sc.parallelize(texts)
# dataset = dataset.zipWithIndex()
#cv=CountVectorizer(inputCol="words",outputCol="features",vocabSize=1000,minDF=2.0)

model=CountVectorizerModel.load("hdfs:/User/"+user+"/"+user+"_cv.model")
result=model.transform(df)
#result.show()
voclist=model.vocabulary

# for x in df.collect():
# 	print("#######################################################")
# 	print(x)
#lda = LDA(k=3,maxIter=10)
#lda.save("hdfs:/"+user+"_text.model")
# print("ss")
#ldamodel =lda.fit(result)
ldamodel=LocalLDAModel.load("hdfs:/User/"+user+"/"+user+"_lda.model")
# ll=model.logLikelihood(dataset)
# lp=model.logPerplexity(dataset)
# print("ll"+str(ll))
# print("up"+str(lp))
topic_wordList=ldamodel.describeTopics(10).select('termIndices').collect()
#print(topic_wordList)
count=0
print('00000\n')
for i in topic_wordList:
	count+=1
	print("topic"+str(count)+": ")
	for k in i.termIndices:
		print(voclist[k],end=' ')
	print()
print('00000\n')
topics=ldamodel.transform(result)
#print(type(topics.select('id','topicDistribution')))
#print(topics.select('topicDistribution').collect())
print('88888\n')
count=0
for i in topics.select('topicDistribution').collect():
	print(filepath[count].split("/")[-1],end=' ')
	print(i.topicDistribution)
	count+=1
print('88888\n')
# transformed=model.transform(dataset)
# transformed.show(truncate=False)
