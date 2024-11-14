import re
import requests
from xml.etree import ElementTree

# using https://www.livetracklist.com/source/boiler-room 
# and the site map (https://www.livetracklist.com/sitemap-page-n.xml)
# as a resource to get tracklists

# step 1 - crawl site map looking for boiler-room sets


class LiveTrackListPage():
    def __init__(self, url: str):
        self.url = url
        self.response = requests.get(url=url)  # move into function and pass into object

        self.data = self.response._content


#TODO main function
regex_matcher = re.compile("boiler\-room|boiler|room")

boiler_room_urls = []
for i in range(12):
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




