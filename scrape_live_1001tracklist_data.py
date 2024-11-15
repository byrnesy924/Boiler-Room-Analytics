import re
import requests
import pandas as pd
import logging
from multiprocessing import Pool
from time import sleep
from xml.etree import ElementTree
from bs4 import BeautifulSoup

logger = logging.basicConfig(filename='1001Tracklist.log', level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

# using https://www.1001tracklists.com/source/gpcruv/boiler-room/index.html
# and the site map
# as a resource to get tracklists

# step 1 - crawl site map looking for boiler-room sets


class OneThousandOneTrackListPage():
    def __init__(self, url: str, headers: dict, set_name: str, dj: str):
        self.base_url = "https://www.1001tracklists.com/"
        self.url = url
        self.response = requests.get(url=self.base_url + url, headers=headers)  # move into function and pass into object

        if self.response.status_code == 200:
            with open("1001_tracklist_htmls/" + set_name + ".txt", "w") as data:
                # 1001 tracklist doesnt always play ball with lots of requests, so save locally
                data.write(self.response.text)
                html_text = self.response.text
        elif self.response.status_code == 429:
            # terrible way to quickly get around 429 - just pause and continue later.
            # because this is multiprocessing this will block one of the processes, but the others can proceed
            # would be much better to write this asynchronously but I'm trying to move fast TODO
            sleep(10)
            self.response = requests.get(url=self.base_url + url, headers=headers)
        else:
            logging.warning(f"Received a failed status code: {self.response.status_code}\n\n {self.response.text}")
            try:
                with open(set_name + ".txt", "r") as data:
                    html_text = data.read()
            except FileNotFoundError:
                logging.exception(f"Response caused failure as file doesnt exist:\n\n{set_name}\n\n{self.response}")

        self.soup = BeautifulSoup(html_text, 'html.parser')
        self.regex_remove_tags = re.compile(r"<[^>]*>")
        self.regex_get_artist = re.compile(r"\s\@.*")  # use that the format is Artist @ Boiler Room

        self.dj = dj

    def get_date_from_set_name(self, url):
        """On 1001 setlists generally the date is in the url at the end. Use regex to get the date"""
        regex_get_url_finish = re.compile("(.*\/)*")
        regex_get_date = re.compile("[0-9]{4}\-[0-9]{2}\-[0-9]{2}")

        url_finish = regex_get_url_finish.sub("", url)
        return regex_get_date.findall(url_finish)
    
    def find_track_data(self, soup):
        """Get list of tracks from html soup"""

        raw_tracks = soup.find_all("div", itemprop="tracks")
        tracks = [{
            "TrackName": self.get_content_from_tag(track.find("meta", itemprop="name")),
            "Artist": self.get_content_from_tag(track.find("meta", itemprop="byArtist")),
            "Genre": self.get_content_from_tag(track.find("meta", itemprop="genre")),
            "Number": index+1
        } for index, track in enumerate(raw_tracks)]
        # If error - create function that can handle missing tags
        return tracks

    def get_content_from_tag(self, tag):
        """Helper function to handle when a meta item for a track is missing"""
        if tag is None:
            return None
        return tag["content"]

    def get_set_information(self) -> pd.DataFrame:
        "main API function to get a pandas dataframe of info"
        tracks = self.find_track_data(self.soup)
        date = self.get_date_from_set_name(url=self.url)

        df = pd.DataFrame(tracks)
        df["DJ"] = self.dj
        df["Date"] = date

        return df


def get_urls_for_boiler_rooms(headers: str):
    regex_matcher = re.compile("boiler\-room|boiler|room")

    # Get html of index page
    site_map_url = "https://www.1001tracklists.com/source/gpcruv/boiler-room/index.html"

    site_map_page_request = requests.get(url=site_map_url, headers=headers)
    # unsure of problem
    site_map_page_request.status_code = -1
    if site_map_page_request.status_code == 200:
        site_map_page_text = site_map_page_request.text
    else:
        # 1001 tracklists was not playing ball with requests - saved index manually
        logging.warning(f"Request failed! Request dump:{site_map_page_request}")
        with open("1001Tracklist-BR-Index.html") as index_page:
            site_map_page_text = index_page.read()

    site_map_soup = BeautifulSoup(site_map_page_text, "html.parser")

    # parse html - find all aref
    all_hyperlinks = site_map_soup.find_all("a")

    # abuse that the @ symbol is put in all the names of the tracks - ignore all other hyperlinks
    br_hyperlinks = [link for link in all_hyperlinks if "@" in link.get_text()]

    # TODO - validate that all have "Boiler Room" in the name
    # use regex to remove everything after the @ - before is DJ name
    regex_isolate_artist = re.compile("\s\@.*")
    # format into
    br_sets = [{"DJ": regex_isolate_artist.sub("", link.get_text()),
                "url": link.get("href"),
                "Set": link.get_text()
                } for link in br_hyperlinks]

    return br_sets


def multiprocessing_wrapper(set: dict, headers: dict):
    headers["path"] = set["url"]
    return OneThousandOneTrackListPage(url=set["url"], set_name=set["Set"], headers=headers, dj=set["DJ"])


def create_db_of_tracklists(br_urls: list, headers: dict):
    # can be paralelised
    args = zip(br_urls, [headers]*len(br_urls))
    with Pool(processes=4) as pool:
        dfs = pool.starmap(multiprocessing_wrapper, args)

    return pd.concat(dfs)


if __name__ == "__main__":

    # Hack - 1001 tracklists gives 404 on get requests, imitate a browser with the following headers
    headers = {
        "authority": "www.1001tracklists.com",
        "method": "GET",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Connection": "keep-alive"
    }

    sets = get_urls_for_boiler_rooms(headers=headers)
    if len(sets) == 0:
        raise Exception("Sets is empty - request and load failed")

    # df = create_db_of_tracklists(br_urls=sets, headers=headers)
    dfs = [multiprocessing_wrapper]

    df.to_csv("live_tracklist_set_lists.csv", index=False)