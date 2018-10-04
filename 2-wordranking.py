
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


# In[20]:


# Getting relevant columns - last two cointain review and rating - also remove puncutations
word_counted =  Amazon.map(lambda x: (x[-1], x[-2].translate({ord(char): None for char in string.punctuation})))


# In[21]:


#combining all comments for the same key
word_counted_1  = word_counted.reduceByKey(lambda x,y : x+y)


# In[22]:


#QUESTION 2


# In[23]:


# Stopwords to remove common words
stopwords  = ['i','me','my','myself','we','our','ours','ourselves','you','your','yours','yourself','yourselves','he','him','his','himself','she','her','hers','herself','it','its','itself','they','them','their','theirs','themselves','what','which','who','whom','this','that','these','those','am','is','are','was','were','be','been','being','have','has','had','having','do','does','did','doing','a','an','the','and','but','if','or','because','as','until','while','of','at','by','for','with','about','against','between','into','through','during','before','after','above','below','to','from','up','down','in','out','on','off','over','under','again','further','then','once','here','there','when','where','why','how','all','any','both','each','few','more','most','other','some','such','no','nor','not','only','own','same','so','than','too','very','s','t','can','will','just','don','should','now']


# In[24]:


# Removing stopwords
word_top = word_counted_1.map(lambda x : (x[0], [y for y in x[1].split() if not y.lower() in stopwords]))


# In[25]:


#Taking frequency of words and 10 taking most common
Word_top_1 = word_top.map(lambda x : (x[0], collections.Counter(x[1]).most_common(10)))


# In[26]:


#Getting only word and removing their freq 
Word_top_2 = Word_top_1 .map(lambda x : (x[0], [y[0] for y in x[1]])).sortByKey()


# In[27]:


Word_top_2.collect()

