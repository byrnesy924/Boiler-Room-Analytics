import os
import re
import requests
import pandas as pd
import logging
from multiprocessing import Pool
from time import sleep
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
logging.basicConfig(filename='scrape_live_1001tracklist_data.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filemode="w")

# using https://www.1001tracklists.com/source/gpcruv/boiler-room/index.html
# and the site map
# as a resource to get tracklists

# step 1 - crawl site map looking for boiler-room sets


class OneThousandOneTrackListPage():
    def __init__(self, url: str, headers: dict, set_name: str, dj: str):
        self.base_url = "https://www.1001tracklists.com/"
        self.url = url
        self.regex_clean_set_name = re.compile(r'\/|\||\#|\&|\(|\)|\"')
        self.storage_file = self.regex_clean_set_name.sub("", set_name)
        self.file_location = "1001_tracklist_htmls/" + self.storage_file + ".html"

        # 17 November - if found locally, then load locally, otherwise use response
        if os.path.isfile(self.file_location):
            try:
                with open(self.file_location, "r", errors="replace") as data:
                    html_text = data.read()
                    logger.info(f"Read data locally for set: {set_name}")
            except FileNotFoundError:
                logger.exception(f"Response caused failure as file doesnt exist:\n\n{set_name}")
                return
        else:
            # Otherwise, send request
            sleep(1)  # trying not to get blacklisted by 1001 tacklists Would be great to async all this but I have not written it well
            # do this way as 1001 tracklists is liable to give 403s or 429s for many requests
            self.response = requests.get(url=self.base_url + url, headers=headers)  # move into function and pass into object
            logger.info(f"No local file found for: {set_name}. Requesting data from 1001 tracklists")
            # Good status code - save data
            if self.response.status_code == 200:
                with open(self.file_location, "w", errors="replace") as data:
                    # 1001 tracklist doesnt always play ball with lots of requests, so save locally
                    data.write(self.response.text)
                    html_text = self.response.text
                logger.info(f"Sucessful DL and write of html for: {set_name}")
            elif self.response.status_code == 429:
                # terrible way to quickly get around 429 - just pause and continue later.
                # because this is multiprocessing this will block one of the processes, but the others can proceed
                # would be much better to write this asynchronously but I'm trying to move fast TODO
                sleep(10)
                self.response = requests.get(url=self.base_url + url, headers=headers)
                html_text = self.response.text
                logger.warning(f"429 Error time out for: {set_name}; Response on retry:{self.response.status_code}")
            else:
                logger.exception(f"Received a failed status code for {set_name}: {self.response.status_code}")
                html_text = ""  # TODO handle this correctly

        self.soup = BeautifulSoup(html_text, 'html.parser')
        self.regex_remove_tags = re.compile(r"<[^>]*>")
        self.regex_get_artist = re.compile(r"\s\@.*")  # use that the format is Artist @ Boiler Room

        self.dj = dj

    def get_date_from_set_name(self, url):
        """On 1001 setlists generally the date is in the url at the end. Use regex to get the date"""
        regex_get_url_finish = re.compile(r"(.*\/)*")
        regex_get_date = re.compile(r"[0-9]{4}\-[0-9]{2}\-[0-9]{2}")

        url_finish = regex_get_url_finish.sub("", url)
        return regex_get_date.findall(url_finish)[0]  # assume that first date is correct

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
        if not hasattr(self, "soup"):
            logger.warning(f"No soup for this object - likely the get request failed.\n set: {self.url}\nResponse code:\n{self.response.status_code}")
            print(f"No soup for this object - likely the get request failed.\n set: {self.url}\nResponse code:\n{self.response.status_code}")
            return None

        tracks = self.find_track_data(self.soup)
        date = self.get_date_from_set_name(url=self.url)

        df = pd.DataFrame(tracks)
        df["DJ"] = self.dj
        df["Date"] = date

        return df


def get_urls_for_boiler_rooms(headers: str):
    # regex_matcher = re.compile("boiler\-room|boiler|room")

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
        with open("1001Tracklist-BR-Index.html",  errors="replace") as index_page:
            site_map_page_text = index_page.read()

    site_map_soup = BeautifulSoup(site_map_page_text, "html.parser")

    # parse html - find all aref
    all_hyperlinks = site_map_soup.find_all("a")

    # abuse that the @ symbol is put in all the names of the tracks - ignore all other hyperlinks
    br_hyperlinks = [link for link in all_hyperlinks if "@" in link.get_text()]

    # TODO - validate that all have "Boiler Room" in the name
    # use regex to remove everything after the @ - before is DJ name
    regex_isolate_artist = re.compile(r"\s\@.*")
    # format into
    br_sets = [{"DJ": regex_isolate_artist.sub("", link.get_text()),
                "url": link.get("href"),
                "Set": link.get_text()
                } for link in br_hyperlinks]

    return br_sets


def multiprocessing_wrapper(set: dict, headers: dict):
    headers["path"] = set["url"]
    return OneThousandOneTrackListPage(url=set["url"], set_name=set["Set"], headers=headers, dj=set["DJ"]).get_set_information()


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
        "Connection": "keep-alive",
        "Cookie": "__qca=P0-820745957-1730510035946; guid=182cd3a595868; cookie=67f919b8-748c-4374-af2a-01fd8441f9ce; cookie_cst=zix7LPQsHA%3D%3D; _lr_env_src_ats=false; _cc_id=a939ad4a21d176c1613a23d09c2dd280; _au_1d=AU1D-0100-001730510032-PNJ3K11R-9Q0Z; ccuid=a0ce8c1d-1a64-4ce3-b6f8-4e340ac33f25; __browsiUID=3947c912-37f2-4c47-8b68-d4aa47c6dabc; __qca=P0-820745957-1730510035946; _ga=GA1.1.557609463.1730510128; panoramaId_expiry=1732246617834; panoramaId=244ad9e18af7acbe9d63ca051aba4945a7023f9f254678cec2c592fc994c43d7; panoramaIdType=panoIndiv; uuid=30CD4DF9-5B15-4E0D-923C-1F8621C78EBD; rl_anonymous_id=RS_ENC_v3_IjAzMDlkZDNiLTkxYjItNDQ5MS1iOWVhLTMzNTkxNjk3OWM0ZSI%3D; rl_page_init_referrer=RS_ENC_v3_IiRkaXJlY3Qi; rl_user_id=RS_ENC_v3_ImQzOGFmOTI0NzZjODM3NmMzMGYwYzE0NGMxMjk2NzhmZGU5ZTczNjQi; rl_session=RS_ENC_v3_eyJpZCI6MTczMTY0OTI3MDU5NSwiZXhwaXJlc0F0IjoxNzMxNjUxMDcwNjA2LCJ0aW1lb3V0IjoxODAwMDAwLCJhdXRvVHJhY2siOnRydWUsInNlc3Npb25TdGFydCI6dHJ1ZX0%3D; rl_trait=RS_ENC_v3_eyJydWRkZXJJZCI6IjJVQVBxY0tsYUFqTHQwa0poTTJ1YzNoWUpmNCIsImNhbXBhaWduIjoiMzI1NTkyMDAiLCJhZHYiOiIxMjUzNjMxNCIsInAiOiI0MDY0MTI5MjMiLCJjIjoiMjIzMDk1NTU1IiwiYWQiOiI1OTYzNDk0MDMiLCJlbnYiOiJqIiwicm5kIjoiMTAxMDM4NjUyOSIsInV1aWQiOiIxNTUyMGVmNi1mNjUwLTRjMzQtODdkMi1mZTY3ODEzMjE2MmEiLCJhY3Rpb24iOiJpbXByZXNzaW9uIiwib3JpZ2lucyI6Imh0dHBzOi8vd3d3LjEwMDF0cmFja2xpc3RzLmNvbSxodHRwczovL3d3dy4xMDAxdHJhY2tsaXN0cy5jb20ifQ%3D%3D; _lr_retry_request=true; connectId=%7B%22vmuid%22%3A%22nYw2SjpWLcI8tzkZFJO9t5AT_PFVjqVeI3HtjR8uRQ_zhq_s8Onlw0Nt7meczY2GNP0LS32mtcpfl2ROmRWZQA%22%2C%22connectid%22%3A%22nYw2SjpWLcI8tzkZFJO9t5AT_PFVjqVeI3HtjR8uRQ_zhq_s8Onlw0Nt7meczY2GNP0LS32mtcpfl2ROmRWZQA%22%2C%22connectId%22%3A%22nYw2SjpWLcI8tzkZFJO9t5AT_PFVjqVeI3HtjR8uRQ_zhq_s8Onlw0Nt7meczY2GNP0LS32mtcpfl2ROmRWZQA%22%2C%22ttl%22%3A86400000%2C%22lastSynced%22%3A1732013392484%2C%22lastUsed%22%3A1732013410284%7D; _ga_FVWZ0RM4DH=GS1.1.1732013352.7.1.1732013416.60.0.0; cto_bundle=tXSRZl9pOFI4aFE4N2ZrZG42UkVCVzZ5V3JuRFFNcDRaZEUlMkJ4MDFIZG52UWdzJTJGZ1B0dEQ3Vnl4ZUNvbktPRmVqQm1rdURZeGUxUkhZTHQwbVZRQnlJdHpJYUtBWVpsNW92S1ZYclpFUFFOVWdaOFRZVG05bjFKWXBmV3prM1lDeDElMkJqWGRWTjhoY0Jhenp1VDFOJTJCazNYJTJGUjNscjFBJTJCTUM1NWdmaXdqN1pwbiUyRlpkYyUzRA; __gads=ID=2d8730b0bb5c4846:T=1730510033:RT=1732016048:S=ALNI_MbmXAwUeYEfUNmb0EsfqQoSSLDLuw; __gpi=UID=00000f5d9c477e5a:T=1730510033:RT=1732016048:S=ALNI_Ma2FV8skRTR4UE1YsUgfJvV0E6tFA; __eoi=ID=398cba7e0c60d090:T=1730510033:RT=1732016048:S=AA-AfjY2swsSuc8pyzj5-wPCSGNa",
        "dnt": "1",
        "sec-ch-ua": '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "Windows",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1"
    }

    sets = get_urls_for_boiler_rooms(headers=headers)
    logger.info(f"Number of sets found:{len(sets)}")
    logger.info(f"Sets to do:{sets}")
    if len(sets) == 0:
        raise Exception("Sets is empty - request and load failed")

    # df = create_db_of_tracklists(br_urls=sets, headers=headers)
    dfs = [multiprocessing_wrapper(set=br_set, headers=headers) for br_set in sets]
    df = pd.concat(dfs)

    df.to_csv("1001_tracklist_set_lists.csv", index=False, encoding="utf-32")