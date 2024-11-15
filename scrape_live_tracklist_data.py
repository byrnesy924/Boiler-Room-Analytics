import re
import requests
import pandas as pd
from multiprocessing import Pool
from xml.etree import ElementTree
from bs4 import BeautifulSoup


# using https://www.livetracklist.com/source/boiler-room 
# and the site map (https://www.livetracklist.com/sitemap-page-n.xml)
# as a resource to get tracklists

# step 1 - crawl site map looking for boiler-room sets


class LiveTrackListPage():
    def __init__(self, url: str):
        self.url = url
        self.response = requests.get(url=url)  # move into function and pass into object

        self.soup = BeautifulSoup(self.response.text, 'html.parser')
        self.regex_remove_tags = re.compile(r"<[^>]*>")
        self.regex_get_artist = re.compile(r"\s\@.*")  # use that the format is Artist @ Boiler Room

    def get_artist(self):
        """Wrapper for beautiful soup to return artist"""
        heading = self.soup.find("h1")
        return self.regex_get_artist.sub("", heading.text)

    def return_all_lists_for_date(self):
        """Wrapper for beautiful soup to find dates"""
        return self.soup.find_all('span', class_='list-item')

    def return_table_of_songs(self):
        """Wrapper for beautiful soup to find table of tracks"""
        return self.soup.find_all("div", class_="track-row")

    def extract_track_info(self, track_info):
        """Handles individual rows in the table of records"""
        track_number = track_info.find('span', class_="track-number")
        track_artist = track_info.find('span', class_="artist")
        track_name = track_info.find('span', class_="title")

        if track_number is not None:
            track_number = track_number.text
        if track_artist is not None:
            track_artist = track_artist.text
        if track_name is not None:
            track_name = track_name.text

        return {"Number":  track_number,
                "Artist": track_artist,
                "TrackName": track_name
                }

    def get_formatted_table_of_tracks(self):
        """Final function to call to get table of tracks"""
        list_of_tracks = self.return_table_of_songs()
        return [self.extract_track_info(info) for info in list_of_tracks]

    def get_date_of_set(self, html_list_items_on_webpage: list):
        # regex for removing HTML tags
        list_of_removed_tags = [entry.text for entry in html_list_items_on_webpage]

        # if there is just one - return that value
        if len(list_of_removed_tags) == 1:
            return list_of_removed_tags[0]

        # if there is multiple, try use regex to find the date
        regex_date = re.compile("(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s[0-9]{2}\,\s[0-9]{4}")
        for text_value in list_of_removed_tags:
            if regex_date.match(text_value):
                return text_value
        return None
    
    def get_set_information(self) -> pd.DataFrame:
        "main API function to get a pandas dataframe of info"
        date = self.get_date_of_set(self.return_all_lists_for_date())
        table = self.get_formatted_table_of_tracks()
        artist = self.get_artist()

        df = pd.DataFrame(table)
        df["DJ"] = artist
        df["Date"] = date

        return df

        
def get_urls_for_boiler_rooms():
    regex_matcher = re.compile("boiler\-room|boiler|room")

    boiler_room_urls = []
    for i in range(13):  # Hardcoded for point in time
        site_map_url = f"https://www.livetracklist.com/sitemap-page-{i+1}.xml"

        site_map_page_request = requests.get(url=site_map_url)
        
        # parse xml
        site_map_events = ElementTree.fromstring(site_map_page_request.content)

        # search for "boiler" "boiler-room" "boiler_room" etc. in URLs
        for event, elem in site_map_events:
            # text for the URLs is in the event
            if len(regex_matcher.findall(event.text)) > 0:
                # add to list of URLs to get
                boiler_room_urls.append(event.text)
    return boiler_room_urls


def multiprocessing_wrapper(url):
    return LiveTrackListPage(url).get_set_information()


def create_db_of_tracklists(br_urls: list):
    # can be paralelised
    with Pool(processes=4) as pool:
        dfs = pool.map(multiprocessing_wrapper, br_urls)

    return pd.concat(dfs)


if __name__ == "__main__":
    boiler_rooms = get_urls_for_boiler_rooms()

    df = create_db_of_tracklists(boiler_rooms)

    df.to_csv("live_tracklist_set_lists.csv", index=False)


