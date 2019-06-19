# Bubble Breaker

# Introduction

 Social tribalism is a problem in this age of social media. The first step towards breaking the "filter bubble" is by recognizing the existence of the bubble, and that there is not a single view but a spectrum of views of things. To help break the bubble, this project provides users with a frontend to query selected popular new topics, and access the sentiments surrounding this topic from a variety of news source. This grants users awareness of the full spectrum of views, and brings important links to the news to the user.

# DATA
 The Global Database of Events, Language, and Tone (GDELT) dataset gathers news reports from all over the globe, and identifies the people, locations, organizations, themes, sources, emotions, counts, quotes, images and events involved. The dataset amounts to ~2.5TB per year, and is updated every 15 minute.


## Data Pipeline:

![](./images/bubble-breaker-pipeline.png)

## Data Processing:

 The event data is extracted from S3 datasource and cleansed. The data is transformed to perform calculation of critical statistics and loaded to Cassandra. The flask web layer servers the analytics to display the frontend.
