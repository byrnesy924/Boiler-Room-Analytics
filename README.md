# Boiler-Room-Analytics
This repository contains a Community Analysis of the music played in Boiler Room DJ sets over time. In the future, I intent to mine out the genre of each community with NLP techniques.

Click on the graphic below to check out and interact the network of artists
<table style="width:100%;">
  <tr valign="top">
    <td align="center"><a href="https://hub.graphistry.com/graph/graph.html?dataset=a9604b01b4a84a0a82d1dba61fccb4d0&type=arrow&viztoken=c6c33a2d-984b-486b-955b-19e2ca78510b&usertag=4a751847-pygraphistry-0.34.17&splashAfter=1736163734&info=False&menu=False&showArrows=False&pointSize=0.7&edgeCurvature=0.0&edgeOpacity=0.5&pointOpacity=0.9" target="_blank"><img src="Images/br_analytics_screenshot.png" title="Click to open"></a>
    <a href="https://hub.graphistry.com/graph/graph.html?dataset=dc93907254204c92a86b67fa958ee0c8" target="_blank">Demo: Interactive visualisation of Boiler Room tracklists</a> 
    </td>
  </tr>
</table>

## Findings and Analysis
TODO

## Data Source
I scraped tracklist data from 1001 Tracklists - see the [code structure](#code-structure) below. 1001 Tracklists contains tracklist data from shows and DJ sets maintained by fans and listeners, and for sets played at Boiler Rooms goes back to 2010. Their website also uses a number of APIs (e.g. beatport) for track information. Given it is community maintained, data quality is *very patchy*, e.g., genres are often missing, Artists' spelling can change, and so on.

I also used Spotify's and Discogs' respective APIs to retrieve more genre data for the NLP part of the project. Three notes:
- Spotify currently only provides genres for **Artists** rather than individual songs. This made searching easier, but limits the precision of the following NLP analysis. Given the communities are clusters of artists, I am not bothered by this.
- Discogs does not seem to have poor precision on genres within dance music - often songs will only be tagged with "Electronic" (pending results of data collection, as of writing my internet is too bad where I am to actually perform this analysis) 
- Given the tracklist data quality is low in the first place, searching for for tracks and Artists (and especially remixes or edits) on the two platforms is made difficult, further limiting quality.

## Code structure
Given this was a relatively quick-and-dirty analysis mostly done for my interest and a bit of novelty, the code is not structured or written particularly elegantly.
I initially took a scripting approach, with the scripts operating in order. [Main.py](Main.py) wraps all these scripts and runs them together (see [Running Locally](#running-locally))

#### 1. Scrape Tracklist data from 1001 tracklist
The file [scrape_live_1001tracklist_data.py](scrape_live_1001tracklist_data.py) extracts the data from 1001 Tracklists.
To do so, I found an index page for Boiler Room sets on 1001 Tracklist. The function `get_urls_for_boiler_rooms` extacts each of these sets and their URL. The object `OneThousandOneTrackListPage` and its methods contain the behaviour for extracting the traclist.

#### 2. Clean Data and perform Fuzzy Mutching
The [clean data script](clean_br_data.py) is not particularly elegant, and performs a few functions:
- It performs some initial string cleaning and processing - fixing encoding errors, removing punctuation and words like Edit/Remix/Bootleg, and so on.
- It also splits out all the artists/remixers etc. for a song. In the final analysis, all artists involved in a track being played (the DJ, the Artist(s), the Remixer(s)) will have an edge drawn between them. Doing this also aids in string cleaning
- It then takes all the unique Artists (no matter what their envolvment) and performs a fuzzy matching exercise on all artists to all other artists. Any artist pairs with more than an 80% string similarity are merged together

I took the fuzzy matching approach to amend some of the data quality issues found in the 1001 tracklist data. Fuzzy matching is often not very precise (hence the name): setting the threshold too low will lead to completely different artists being merged together; setting it too high will limit the number of merges for artists that should be merged. In other words, you must trade off false-positives and false-negatives.
In this context, over-merging artists is a greater risk. This could lead to additional "bridge" connections between communities in the graph, or incorrect artists being placed into communities, which would make communitiy detection more difficult, less precise, and less accurate.
I performed a brief analysis of the number/proportion of artists caught by the 80% threshold and did some manual review of the artists merged to validate the results (see [this file](Images/histogram_of_similarity_values.png) and [this file below]())

[![Histogram of string similarity results for all combinations of artists](Images/histogram_of_similarity_values.png)](Images/histogram_of_similarity_values.png)
[![The tail of the histogram for detailed analysis](Images/tail_of_histogram.png)](Images/tail_of_histogram.png)

#### 3. Retrieve Genre data from Spotify and Discogs
Both of these services have APIs for searching for tracks and retrieving data on those results. Both also have python packages for retrieving this data, which I have used: [Spotipy](https://github.com/spotipy-dev/spotipy) and [Discogs client](https://github.com/joalla/discogs_client). The documentation for these projects was easy enough to use.
Note that as said above, the data quality is limited, both by the data quality upstream from 1001 Tracklists, and from these sources themselves.

#### 4. Graph Aanlytics
I've used NetworkX for identifying the Louvain communities within the graph, and Graphistry for visualisation. In my experiences, graphistry can visualise large graphs better than most python specific graph analytics packages, although the aesthetic of Graphistry can be underwhelming. In the future, I want to attempt the viualisation again with Neo4J. 

## ToDo List
- When internet is better take a screenshot of graph for README
- Summarise findings
- Perform genre data extraction from Spotify and Discogs (internet where I am is so bad I cannot maintain a connection for long enough)
  - asses data quality, clean if necessary
- design and perform NLP analysis for communities
- intergate two analyses
- Experiment with the Leiden algo for community detection rather than Louvain
- Re-implement the graph visualisation with Neo4J

## Running Locally
You will need your own Graphistry account set up, with details saved in a .env. Similarly, for Discogs and Spotify's APIs, you will need to set up authorisation and save those in a .env.

Clone the repository and run
```
pip install requirements.txt
python main.py
```

