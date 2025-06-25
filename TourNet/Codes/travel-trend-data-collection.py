import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import json
import re
from datetime import datetime
import logging
from typing import List, Dict
import tweepy

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TravelDataCollector:
    def __init__(self, twitter_bearer_token=None):
        self.target_hashtags = ['#travel', '#wanderlust', '#travelgram', '#vacation', '#adventure']
        self.tweepy_client = None

        if twitter_bearer_token:
            self.tweepy_client = tweepy.Client(bearer_token=twitter_bearer_token, wait_on_rate_limit=True)
            logger.info("Twitter API v2 client initialized")

    def collect_twitter_data_v2(self, hashtag: str, limit: int = 100) -> List[Dict]:
        if not self.tweepy_client:
            return []

        tweets_data = []
        try:
            tweets = tweepy.Paginator(
                self.tweepy_client.search_recent_tweets,
                query=f"{hashtag} -is:retweet lang:en",
                tweet_fields=['created_at', 'author_id', 'public_metrics', 'entities', 'geo'],
                expansions=['author_id', 'geo.place_id'],
                user_fields=['username'],
                place_fields=['full_name'],
                max_results=min(limit, 100)
            ).flatten(limit=limit)

            for tweet in tweets:
                hashtags = [f"#{tag['tag']}" for tag in tweet.entities.get('hashtags', [])] if tweet.entities else []
                metrics = tweet.public_metrics or {}
                location = tweet.geo.get('place_id') if tweet.geo else None

                tweets_data.append({
                    'platform': 'Twitter',
                    'post_text': tweet.text,
                    'username': f"user_{tweet.author_id}",
                    'timestamp': tweet.created_at.isoformat() if tweet.created_at else '',
                    'hashtags': hashtags,
                    'location': location or '',
                    'likes': metrics.get('like_count', 0),
                    'retweets': metrics.get('retweet_count', 0),
                    'replies': metrics.get('reply_count', 0),
                    'engagement': sum([metrics.get(k, 0) for k in ['like_count', 'retweet_count', 'reply_count']]),
                    'url': f"https://twitter.com/i/web/status/{tweet.id}",
                    'search_hashtag': hashtag
                })

            logger.info(f"Collected {len(tweets_data)} tweets for {hashtag}")
        except tweepy.TooManyRequests:
            logger.warning(f"Rate limit hit for {hashtag}. Waiting for 2 minutes...")
            time.sleep(120)
            return self.collect_twitter_data_v2(hashtag, limit)
        except Exception as e:
            logger.error(f"Error collecting tweets for {hashtag}: {e}")
        return tweets_data

    def scrape_blog_simple(self, url: str) -> List[Dict]:
        posts = []
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': 'https://www.google.com/'
            }
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            for selector in ['article', '.post', '.blog-post', '.entry']:
                elements = soup.select(selector)
                if elements:
                    for elem in elements[:5]:
                        title = elem.find(['h1', 'h2', 'h3'])
                        title_text = title.get_text(strip=True) if title else "No title"
                        content = elem.get_text(strip=True)[:500]
                        travel_keywords = ['travel', 'trip', 'vacation', 'journey', 'destination']
                        if any(keyword in content.lower() for keyword in travel_keywords):
                            posts.append({
                                'platform': 'Blog',
                                'post_text': f"{title_text}\n\n{content}",
                                'username': url.split('//')[1].split('/')[0],
                                'timestamp': datetime.now().isoformat(),
                                'hashtags': re.findall(r'#\w+', content),
                                'location': '',
                                'likes': 0,
                                'retweets': 0,
                                'replies': 0,
                                'engagement': 0,
                                'url': url,
                                'search_hashtag': 'blog'
                            })
                    break
            logger.info(f"Collected {len(posts)} posts from {url}")
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
        return posts

    def collect_all_data(self, tweet_limit: int = 50, blog_urls: List[str] = None) -> pd.DataFrame:
        all_data = []

        for hashtag in self.target_hashtags:
            tweets = self.collect_twitter_data_v2(hashtag, tweet_limit)
            all_data.extend(tweets)
            time.sleep(2)  # Avoid rapid hitting API

        if blog_urls:
            for url in blog_urls:
                posts = self.scrape_blog_simple(url)
                all_data.extend(posts)
                time.sleep(2)

        df = pd.DataFrame(all_data)
        if not df.empty:
            df = df.drop_duplicates(subset=['post_text', 'username'], keep='first')
            df['post_length'] = df['post_text'].str.len()
            df['hashtag_count'] = df['hashtags'].apply(len)
            df['engagement_rate'] = df['engagement'] / (df['engagement'].max() + 1)
            logger.info(f"Collected {len(df)} unique posts")
        return df

    def save_data(self, df: pd.DataFrame, filename: str = None):
        if df.empty:
            logger.warning("No data to save")
            return
        if not filename:
            filename = f"travel_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        df.to_csv(f"{filename}.csv", index=False)
        logger.info(f"Data saved to {filename}.csv")
        df.to_json(f"{filename}.json", orient='records', indent=2)
        logger.info(f"Data saved to {filename}.json")
        with open(f"{filename}_summary.txt", 'w') as f:
            f.write(f"Travel Data Collection Summary\n{'='*40}\n\n")
            f.write(f"Total posts: {len(df)}\n")
            f.write(f"Platforms: {', '.join(df['platform'].unique())}\n")
            f.write(f"Top hashtags: {df.explode('hashtags')['hashtags'].value_counts().head(10).to_dict()}\n")
            f.write(f"Average engagement: {df['engagement'].mean():.2f}\n")
        logger.info(f"Summary saved to {filename}_summary.txt")

def main():
    blog_urls = [
        # Original URLs
        "https://www.nomadicmatt.com/travel-blog/",
        "https://expertvagabond.com/",
        "https://www.theblondeabroad.com/blog/",
        "https://www.thebrokebackpacker.com/blog/",
        "https://www.danflyingsolo.com/blog/",
        "https://www.handluggageonly.co.uk/blog/",
        "https://www.worldofwanderlust.com/blog/",
        "https://www.bemytravelmuse.com/blog/",
        "https://adventurouskate.com/",
        "https://www.travelinglifestyle.net/",
        "https://www.earthtrekkers.com/blog/",
        "https://www.traveldudes.com/blog/",
        "https://www.maptrotting.com/",
        "https://www.goworldtravel.com/",
        "https://traveladdicts.net/",
        "https://www.neverendingfootsteps.com/blog/",
        "https://theplanetd.com/blog/",
        "https://www.alexinwanderland.com/blog/",
        "https://www.travelblog.org/",
        "https://ytravelblog.com/blog/",
        "https://onestep4ward.com/",
        "https://www.bucketlistly.blog/",
        "https://www.escapeartistes.com/",
        "https://www.theworldpursuit.com/",
        "https://ordinarytraveler.com/",
        "https://ourawesomeplanet.com/",
        "https://www.theadventurousfeet.com/",
        "https://www.ourtravelpassport.com/blog/",
        "https://www.crazysexyfuntraveler.com/",
        "https://www.chasingthedonkey.com/",
        "https://www.wanderingearl.com/blog/",
        "https://www.livingthedreamrtw.com/",
        "https://www.travelsewhere.net/",
        "https://www.solitarywanderer.com/",
        "https://www.hippie-inheels.com/blog/",
        "https://www.nomadicsamuel.com/blog",
        "https://www.wheressharon.com/",
        "https://www.globotreks.com/blog/",
        "https://www.lifehack.org/articles/lifestyle/travel",
        "https://www.savoredjourneys.com/",
        "https://www.wanderingredhead.com/",
        "https://www.foxnomad.com/",
        "https://www.mytravelbf.com/",
        "https://www.jessieonajourney.com/blog/",
        "https://www.off-the-path.com/en/blog/",
        "https://www.laidbacktrip.com/blog/",
        "https://www.travelingcanucks.com/blog/",
        "https://www.gonewiththewynns.com/blog",
        "https://www.legalnomads.com/blog/",
        "https://www.mrsoaroundtheworld.com/blog/",
        "https://roadbook.travel/",
        "https://27travels.com/",
        "https://www.personal-landscapes.com/blog/",
        "https://roadsandkingdoms.com/",
        "https://perceptivetravel.com/",
        "https://themaninseat61.com/",
        "https://afar.com/",
        "https://getlostmagazine.com/",
        "https://notesfromtheroad.com/",
        "https://spottedbylocals.com/",
        "https://journeyera.com/",
        "https://atlasandboots.com/",
        "https://fearful-adventurer.com/",
        "https://goatsontheroad.com/",
        "https://theroadtripexpert.com/",
        "https://wildjunket.com/",
        "https://gonomad.com/",
        "https://uncorneredmarket.com/",
        "https://travelnoire.com/",
        "https://theculturetrip.com/",
        "https://thetraveltester.com/",
        "https://nomadicvegan.com/",
        "https://thesavvybackpacker.com/",
        "https://onlyinyourstate.com/",
        "https://aedreams.com/",
        "https://hawaiiguide.com/",
        "https://luxurytravelmom.com/",
        "https://earthtrekkers.com/",
        "https://globetrekking.com/",
        "https://thepointsguy.com/",
        "https://telegraph.co.uk/travel/",
        "https://cnntravel.com/",
        "https://lonelyplanet.com/blog/",
        "https://matadornetwork.com/",
        "https://culturetrip.com/",
        "https://passport.com/",
        "https://nextstopmagazine.com/",
        "https://independenttraveler.com/",
        "https://theblondesalad.com/travel/",
        "https://onebag.com/blog/",
        "https://theluxurytraveler.com/",
        "https://traveldeeply.com/",
        "https://intrepidtravel.com/adventures/blog/",
        "https://savvytourist.co/",
        "https://wanderlustmovement.org/",
        "https://roamaroundtheplanet.com/",
        "https://thediscoverer.com/",
        "https://expertworldtravel.com/",
        "https://crunchymamastravel.co/",
        
        # Additional 300 URLs
        "https://www.backpackerlee.com/",
        "https://www.heleneinbetween.com/",
        "https://www.clautravels.com/",
        "https://www.solopassport.com/",
        "https://www.themiddleseatblog.com/",
        "https://www.backpackingwithbacon.com/",
        "https://www.theblondetravels.com/",
        "https://www.heartmybackpack.com/",
        "https://www.theroamingboomers.com/",
        "https://www.beersandbeans.com/",
        "https://www.budgetyourtrip.com/",
        "https://www.nomadicnature.com/",
        "https://www.brownboytravel.com/",
        "https://www.travelspock.com/",
        "https://www.wanderingwheatleys.com/",
        "https://www.migrationology.com/",
        "https://www.theinvisibletourist.com/",
        "https://www.johnnyjet.com/",
        "https://www.travelpulse.com/",
        "https://www.travelagentcentral.com/",
        "https://www.worldtravel.com/",
        "https://www.tripadvisor.com/blog/",
        "https://www.expedia.com/blog/",
        "https://www.booking.com/articles/",
        "https://www.airbnb.com/travel/",
        "https://skift.com/",
        "https://www.travelandleisure.com/",
        "https://www.cntraveler.com/",
        "https://www.nationalgeographic.com/travel/",
        "https://www.fodors.com/",
        "https://www.frommers.com/",
        "https://www.roughguides.com/",
        "https://www.lonelyplanet.com/",
        "https://www.timeout.com/",
        "https://www.viator.com/blog/",
        "https://www.getyourguide.com/magazine/",
        "https://www.gadventures.com/blog/",
        "https://www.contiki.com/travel-blog/",
        "https://www.topdeck.travel/blog/",
        "https://www.busabout.com/blog/",
        "https://www.ef.com/blog/",
        "https://www.statravel.com/blog/",
        "https://www.hostelworld.com/blog/",
        "https://www.hostelbookers.com/blog/",
        "https://www.livetheworld.com/",
        "https://www.wanderon.in/blogs/",
        "https://www.thrillophilia.com/blog/",
        "https://www.makemytrip.com/blog/",
        "https://www.goibibo.com/blog/",
        "https://www.cleartrip.com/blog/",
        "https://www.yatra.com/blog/",
        "https://www.easemytrip.com/blog/",
        "https://www.ixigo.com/blog/",
        "https://www.holidayiq.com/blog/",
        "https://www.travelogyindia.com/",
        "https://www.indiamike.com/",
        "https://www.indiatravel.com/blog/",
        "https://www.incredibleindia.org/content/incredible-india-v2/en/blog.html",
        "https://www.tourism.gov.in/",
        "https://www.keralatourism.org/blog/",
        "https://www.rajasthantourism.gov.in/blog/",
        "https://www.gujarattourism.com/blog/",
        "https://www.maharashtratourism.gov.in/blog/",
        "https://www.karnatakatourism.org/blog/",
        "https://www.tamilnadutourism.org/blog/",
        "https://www.andhrapradesh.com/blog/",
        "https://www.telangana.gov.in/blog/",
        "https://www.wbtourism.gov.in/blog/",
        "https://www.biharitourism.gov.in/blog/",
        "https://www.jharkhandtourism.gov.in/blog/",
        "https://www.odishatourism.gov.in/blog/",
        "https://www.chhattisgarhtourism.gov.in/blog/",
        "https://www.mptourism.com/blog/",
        "https://www.uptourism.gov.in/blog/",
        "https://www.delhitourism.gov.in/blog/",
        "https://www.punjabtourism.gov.in/blog/",
        "https://www.haryanatourism.gov.in/blog/",
        "https://www.himachaltourism.gov.in/blog/",
        "https://www.jktourism.jk.gov.in/blog/",
        "https://www.uttarakhandtourism.gov.in/blog/",
        "https://www.assamtourism.gov.in/blog/",
        "https://www.manipurtourism.gov.in/blog/",
        "https://www.mizoramtourism.com/blog/",
        "https://www.nagalandtourism.com/blog/",
        "https://www.tripuratourism.gov.in/blog/",
        "https://www.arunachaltourism.com/blog/",
        "https://www.sikkimtourism.gov.in/blog/",
        "https://www.meghalayatourism.in/blog/",
        "https://www.goatourism.gov.in/blog/",
        "https://www.lakshadweeptourism.com/blog/",
        "https://www.andamantourism.gov.in/blog/",
        "https://www.delhitourism.com/blog/",
        "https://www.chandigarhtourism.gov.in/blog/",
        "https://www.puducherrytourism.gov.in/blog/",
        "https://www.dnh.nic.in/blog/",
        "https://www.dd.nic.in/blog/",
        "https://www.lakshadweep.nic.in/blog/",
        "https://www.traveltriangle.com/blog/",
        "https://www.holidify.com/blog/",
        "https://www.travelogytrips.com/blog/",
        "https://www.tripsavvy.com/",
        "https://www.smartertravel.com/",
        "https://www.budgettravel.com/",
        "https://www.travelblog.com/",
        "https://www.travelchannel.com/",
        "https://www.discoverytravel.com/",
        "https://www.bbctravel.com/",
        "https://www.guardiantravel.com/",
        "https://www.independenttravel.com/",
        "https://www.timesofindiatravel.com/",
        "https://www.hindustantimesravel.com/",
        "https://www.economictimesravel.com/",
        "https://www.ndtvtravel.com/",
        "https://www.news18travel.com/",
        "https://www.republictravel.com/",
        "https://www.indiatodaytravel.com/",
        "https://www.outlooktravel.com/",
        "https://www.theweektravel.com/",
        "https://www.openmyindiatravel.com/",
        "https://www.thehindutravelplus.com/",
        "https://www.deccanchronicletravel.com/",
        "https://www.telegraphinditravel.com/",
        "https://www.statesmantravelplus.com/",
        "https://www.expresstravelworld.com/",
        "https://www.travelbitezindia.com/",
        "https://www.travelmassala.com/",
        "https://www.outlooktraveller.com/",
        "https://www.cntraveller.in/",
        "https://www.travelandleisureindia.in/",
        "https://www.nationalgeographictraveller.in/",
        "https://www.lonelyplanet.com/india/",
        "https://www.roughguides.com/destinations/asia/india/",
        "https://www.fodors.com/world/asia/india/",
        "https://www.frommers.com/destinations/india/",
        "https://www.tripadvisor.in/",
        "https://www.booking.com/country/in.html",
        "https://www.agoda.com/country/india.html",
        "https://www.hotels.com/de1481623/hotels-india/",
        "https://www.expedia.co.in/",
        "https://www.kayak.co.in/",
        "https://www.skyscanner.co.in/",
        "https://www.momondo.in/",
        "https://www.cheapflights.co.in/",
        "https://www.jetstar.com/in/",
        "https://www.airindia.in/",
        "https://www.goair.in/",
        "https://www.spicejet.com/",
        "https://www.indigo.in/",
        "https://www.vistara.com/",
        "https://www.airasiainindia.com/",
        "https://www.jetairways.com/",
        "https://www.bluedarairlines.com/",
        "https://www.allianceair.in/",
        "https://www.trujet.com/",
        "https://www.akasaair.com/",
        "https://www.starair.in/",
        "https://www.flybigg.com/",
        "https://www.heritage-aviation.com/",
        "https://www.club-one-air.com/",
        "https://www.deccan.net/",
        "https://www.paramount-airways.com/",
        "https://www.mdlr.in/",
        "https://www.air-costa.com/",
        "https://www.flywidebody.com/",
        "https://www.neo-sky.com/",
        "https://www.zoom-air.com/",
        "https://www.turbo-megha.com/",
        "https://www.air-one.in/",
        "https://www.airways-india.com/",
        "https://www.jagson-airlines.com/",
        "https://www.modiluft.com/",
        "https://www.damania.net/",
        "https://www.eastwest-airlines.com/",
        "https://www.sahara-airlines.com/",
        "https://www.kingfisher-airlines.com/",
        "https://www.indian-airlines.com/",
        "https://www.pawan-hans.com/",
        "https://www.irctc.co.in/",
        "https://www.indianrailways.gov.in/",
        "https://www.railyatri.in/blog/",
        "https://www.trainman.in/blog/",
        "https://www.confirmtkt.com/blog/",
        "https://www.ixigo.com/trains/blog/",
        "https://www.redbus.in/blog/",
        "https://www.abhibus.com/blog/",
        "https://www.busbud.com/blog/",
        "https://www.oyorooms.com/blog/",
        "https://www.zostel.com/blog/",
        "https://www.treebo.com/blog/",
        "https://www.fabhotels.com/blog/",
        "https://www.goibibo.com/hotels/blog/",
        "https://www.cleartrip.com/hotels/blog/",
        "https://www.yatra.com/hotels/blog/",
        "https://www.makemytrip.com/hotels/blog/",
        "https://www.easemytrip.com/hotels/blog/",
        "https://www.ixigo.com/hotels/blog/",
        "https://www.holidayiq.com/hotels/blog/",
        "https://www.travelogyindia.com/hotels/blog/",
        "https://www.indiamike.com/hotels/blog/",
        "https://www.indiatravel.com/hotels/blog/",
        "https://www.incredibleindia.org/hotels/blog/",
        "https://www.tourism.gov.in/hotels/blog/",
        "https://www.keralatourism.org/hotels/blog/",
        "https://www.rajasthantourism.gov.in/hotels/blog/",
        "https://www.gujarattourism.com/hotels/blog/",
        "https://www.maharashtratourism.gov.in/hotels/blog/",
        "https://www.karnatakatourism.org/hotels/blog/",
        "https://www.tamilnadutourism.org/hotels/blog/",
        "https://www.andhrapradesh.com/hotels/blog/",
        "https://www.telangana.gov.in/hotels/blog/",
        "https://www.wbtourism.gov.in/hotels/blog/",
        "https://www.biharitourism.gov.in/hotels/blog/",
        "https://www.jharkhandtourism.gov.in/hotels/blog/",
        "https://www.odishatourism.gov.in/hotels/blog/",
        "https://www.chhattisgarhtourism.gov.in/hotels/blog/",
        "https://www.mptourism.com/hotels/blog/",
        "https://www.uptourism.gov.in/hotels/blog/",
        "https://www.delhitourism.gov.in/hotels/blog/",
        "https://www.punjabtourism.gov.in/hotels/blog/",
        "https://www.haryanatourism.gov.in/hotels/blog/",
        "https://www.himachaltourism.gov.in/hotels/blog/",
        "https://www.jktourism.jk.gov.in/hotels/blog/",
        "https://www.uttarakhandtourism.gov.in/hotels/blog/",
        "https://www.assamtourism.gov.in/hotels/blog/",
        "https://www.manipurtourism.gov.in/hotels/blog/",
        "https://www.mizoramtourism.com/hotels/blog/",
        "https://www.nagalandtourism.com/hotels/blog/",
        "https://www.tripuratourism.gov.in/hotels/blog/",
        "https://www.arunachaltourism.com/hotels/blog/",
        "https://www.sikkimtourism.gov.in/hotels/blog/",
        "https://www.meghalayatourism.in/hotels/blog/",
        "https://www.goatourism.gov.in/hotels/blog/",
        "https://www.lakshadweeptourism.com/hotels/blog/",
        "https://www.andamantourism.gov.in/hotels/blog/",
        "https://www.delhitourism.com/hotels/blog/",
        "https://www.chandigarhtourism.gov.in/hotels/blog/",
        "https://www.puducherrytourism.gov.in/hotels/blog/",
        "https://www.dnh.nic.in/hotels/blog/",
        "https://www.dd.nic.in/hotels/blog/",
        "https://www.lakshadweep.nic.in/hotels/blog/",
        "https://www.traveltriangle.com/hotels/blog/",
        "https://www.holidify.com/hotels/blog/",
        "https://www.travelogytrips.com/hotels/blog/",
        "https://www.backpackerstory.org/",
        "https://www.saltinourhair.com/",
        "https://www.findingtheuniverse.com/",
        "https://www.nomadicfanatic.com/",
        "https://www.everintransit.com/",
        "https://www.biteableplanet.com/",
        "https://www.gettingstamped.com/",
        "https://www.heckticktravels.com/",
        "https://www.monkboughtt.com/",
        "https://www.passportready.com/",
        "https://www.thetravelmanuel.com/",
        "https://www.wanderlustcrew.com/",
        "https://www.roadaffair.com/",
        "https://www.theadventurejunkies.com/",
        "https://www.mountainiq.com/",
        "https://www.outsideonline.com/",
        "https://www.backpacker.com/",
        "https://www.rei.com/blog/",
        "https://www.patagonia.com/stories/",
        "https://www.thenorthface.com/stories/",
        "https://www.arcteryx.com/us/en/stories/",
        "https://www.columbia.com/stories/",
        "https://www.mammut.com/stories/",
        "https://www.blackdiamond.com/stories/",
        "https://www.petzl.com/stories/",
        "https://www.osprey.com/stories/",
        "https://www.deuter.com/stories/",
        "https://www.gregory.com/stories/",
        "https://www.kelty.com/stories/",
        "https://www.marmot.com/stories/"
    ]


    collector = TravelDataCollector(
        twitter_bearer_token="YOUR_TWITTER_BEARER_TOKEN_HERE"
    )

    try:
        print("Starting data collection...")
        df = collector.collect_all_data(
            tweet_limit=30,
            blog_urls=blog_urls
        )
        if not df.empty:
            print(f"\nCollection complete! Total posts: {len(df)}")
            print(f"Platforms: {', '.join(df['platform'].unique())}")
            print("\nSample data:")
            print(df[['platform', 'username', 'engagement', 'hashtag_count']].head())
            collector.save_data(df)
            print("Data saved successfully!")
        else:
            print("No data collected. Check internet connection or API token.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
