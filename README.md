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

## Data Source
I scraped tracklist data from 1001 Tracklist (see the [code structure](#code-structure) below). 1001 Tracklist contains tracklist data from shows and DJ sets maintained by fans and listeners, and for Boiler Room goes back to 2010. Their website also uses a number of APIs (e.g. beatport) for track information. Given it is community maintained, Data quality is *very patchy*, i.e., genre is often missing, Artists spelling can change, especially with accented characters, and so on.

I also used Spotify's and Discogs' respective APIs to retrieve more genre data for the NLP part of the project. Three notes:
- Spotify currently only provides genres for **Artists** rather than individual songs. This made searching easier, but limits the precision of NLP. Given the communities are clusters of artists, I am not bothered by this.
- Discogs does not seem to have great precision on genres within dance music - often songs will only be tagged with "Electronic" (TODO pending results of data collection, as of writing my internet is too bad where I am to actually perform this analysis) 
- Given the tracklist data quality is low, this made it difficult to search for these tracks and Artists on the two platforms, further limiting quality.

## Code structure

## ToDo List
- Perform genre data extraction from Spotify and Discogs (internet where I am is so bad I cannot maintain a connection for long enough)
  - asses data quality, clean if necessary
- design and perform NLP analysis for communities
- intergate two analyses

## Running Locally

