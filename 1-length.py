
# coding: utf-8

# In[1]:


import findspark
findspark.init('/home/rob/spark')
import pyspark


from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.storagelevel import StorageLevel

import collections
import string
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)


# In[2]:


#Read the file and save it by name Amazon
Amazon = sc.textFile("Amazon_Comments.csv").map(lambda x : x.split("^"))


# In[16]:


# Getting relevant columns - last two cointain review and rating - also remove puncutations
word_counted =  Amazon.map(lambda x: (x[-1], x[-2].translate({ord(char): None for char in string.punctuation})))


# In[17]:


#combining all comments for the same key
word_counted_1  = word_counted.reduceByKey(lambda x,y : x+y)


# In[18]:


####count of freq so that it can be used to get average
word_freq_1  = sc.broadcast(word_counted.countByKey())


# In[19]:


word_counted.countByKey()


# In[24]:


#Getting the average - total length divide by count 
word_avg = word_counted_1.map(lambda x: (x[0], len(x[1].split())/word_freq_1.value[x[0]])).sortByKey()


# In[25]:


word_avg.collect()

