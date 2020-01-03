from pyspark.ml.clustering import LDA
import jieba
import os
from pyspark.sql import SparkSession
from pyspark import SparkContext
import os
from pyspark.ml.feature import CountVectorizer
#preprocessing
jieba.load_userdict('/home/hadoop/dict.txt.big') 
path='/home/hadoop/Data/'
filename=os.listdir(path)
print(filename)
texts=list()
count=0
for i in filename:
	texts.append([])
	print(path+i)
	f=open(path+i,"r",encoding='utf8')
	s=f.read()
	s=s.replace('\n','')
	s=jieba.cut(s,cut_all=False)
	f.close()
	f=open("/home/hadoop/stopword.txt","r",encoding='utf8')
	stopwords=f.read()
	stopwords=stopwords.split('\n')
	f.close()
	wordlist=[]
	for word in s:
		if word not in stopwords:
			wordlist.append(word)
	texts[len(texts)-1].append(count)
	texts[len(texts)-1].append(wordlist)
	count+=1
print(texts)
spark=SparkSession.builder.appName("dataFrame").getOrCreate()
df = spark.createDataFrame(texts, ["id", "words"])

# sc =SparkContext()
# dataset = sc.parallelize(texts)
# dataset = dataset.zipWithIndex()
cv=CountVectorizer(inputCol="words",outputCol="features",vocabSize=1000,minDF=2.0)
model = cv.fit(df)
result=model.transform(df)
result.show()
voclist=model.vocabulary

# for x in df.collect():
# 	print("#######################################################")
# 	print(x)
lda = LDA(k=3,maxIter=500)
# print("ss")
ldamodel =lda.fit(result)
# ll=model.logLikelihood(dataset)
# lp=model.logPerplexity(dataset)
# print("ll"+str(ll))
# print("up"+str(lp))
topic_wordList=ldamodel.describeTopics(10).select('termIndices').collect()
print(topic_wordList)
count=0
for i in topic_wordList:
	count+=1
	print("topic"+str(count)+": ")
	for k in i.termIndices:
		print(voclist[k],end=' ')
	print()
topics=ldamodel.transform(result)
#print(type(topics.select('id','topicDistribution')))
#print(topics.select('topicDistribution').collect())
count=0
for i in topics.select('topicDistribution').collect():
	print(filename[count],end=' ')
	print(i.topicDistribution)
	count+=1

# transformed=model.transform(dataset)
# transformed.show(truncate=False)
