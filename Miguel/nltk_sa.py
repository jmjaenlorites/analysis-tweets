import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from pymongo import MongoClient
import sys

client = MongoClient('localhost', 49000)
db = client['Tweets']
kw = sys.argv[1]
collection = db[kw]

# Extract tweets from MongoDB
allTweets = []
for doc in collection.find():
    allTweets.append(doc)

print("Term: ", kw)
print("Ndocs: ", len(allTweets))

nltk.download('vader_lexicon')
sia = SentimentIntensityAnalyzer()

for msg in allTweets:
  res = sia.polarity_scores(msg['text'])
  print(res)