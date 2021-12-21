import pandas as pd
from tqdm import tqdm
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# this file do the actual sentiment analysis

analyzer = SentimentIntensityAnalyzer()
df = pd.read_csv('comments.csv')

# analyse sentiments of comment
sentiments = []
pbar = tqdm(total=df.shape[0])
for row in df.itertuples():
    sentiments.append(analyzer.polarity_scores(row.comment))
    pbar.update(1)
pbar.close()


# add sentiment value columns to the comments DataFrame
sent_df = pd.DataFrame(sentiments)
ndf = df.iloc[:853]
odf = pd.concat([ndf, sent_df], axis=1)

# Write to file
odf.to_csv('analysed.csv')



