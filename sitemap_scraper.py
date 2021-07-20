import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import json
import database
import os


class Scraper:

    def __init__(self):

        self.base_url = 'https://biometricupdate.com/'
        self.sitemap_url = 'sitemap-index-1.xml'
        self.db = 'biometricupdate.db'
        self.raw_articles_table = 'raw_articles'
        self.enriched_articles_table = 'enriched_articles'

        if os.path.isfile(self.db):  # status report on db tables
            print(database.table_status(self.raw_articles_table))
            print(database.table_status(self.enriched_articles_table))
        else:
            print('Database not found. Initializing database.')
            database.initialize_database()

    def update_sitemap_table(self):

        url = self.base_url + self.sitemap_url
        page = requests.get(url)
        soup = bs(page.content, 'xml')
        sitemapTags = soup.find_all("sitemap")

        sitemap_links = []
        for sitemap in sitemapTags:
            sitemap_links.append(sitemap.findNext("loc").text)

        sitemap_df = database.read_table('sitemap_status')

        # reduce sitemap_links to only those that are incomplete or unrecorded
        if len(sitemap_df) == 0:
            pass
        else:
            sitemaps_complete = set(sitemap_df[sitemap_df['sitemap_status'] == 'complete']['sitemap_link'])
            sitemap_links = list(set(sitemap_links) - sitemaps_complete)

        sitemap_columns = ['page_index', 'page_url', 'sitemap_url', 'page_date']

        for sitemap in sitemap_links:

            sitemap_df = pd.DataFrame()
            print('Scraping links from: ', sitemap)
            page = requests.get(sitemap)
            soup = bs(page.content, 'xml')
            urlTags = soup.find_all("url")
            for urlTag in urlTags:
                sitemap_df_row = pd.DataFrame(columns=sitemap_columns, index=range(0,1))

                url = urlTag.findNext("loc").text
                sitemap_df_row['page_index'] = url[32:]
                sitemap_df_row['page_url'] = url
                sitemap_df_row['sitemap_url'] = sitemap
                sitemap_df_row['page_date'] = urlTag.findNext("lastmod").text
                sitemap_df = sitemap_df.append(sitemap_df_row)

            print(f'...{ len(sitemap_df) } page urls scraped. Writing to database.')
            # need to pull current sitemap data, compare and only write new data
            database.write_sitemap_urls(sitemap, sitemap_df)

            print()

            if len(sitemap_df) == 2000:
                status = 'complete'; print('...Status: sitemap complete.')

            else:
                status = 'incomplete'; print('...Status: sitemap incomplete.')

            database.update_sitemap_status(sitemap, status, table='sitemap_status')

    def scrape_article_content(self, limit=100):

        unpulled_df = database.list_unpulled_articles()
        unpulled_articles = unpulled_df['page_url']

        for link in unpulled_articles[0:limit]:
            article_dict = {}
            article_page = requests.get(link)
            article_soup = bs(article_page.content, "html.parser")
            try:  # if i can't get the json, write the link
                json_file = json.loads(article_soup.find('script', attrs={'type': 'application/ld+json'}).contents[0], strict=False)
                article_dict['article_index'] = link[32:]
                article_dict['headline'] = json_file['headline']
                article_dict['author'] = json_file['author']
                article_dict['datePublished'] = json_file['datePublished']
                article_dict['dateModified'] = json_file['dateModified']
                article_dict['contentCategories'] = json_file['articleSection']
                article_dict['articleBody'] = json_file['articleBody']
                article_dict['link'] = link
            except:
                article_dict['link'] = link

            print(f"Scraping {article_dict['article_index']}")
            database.write_article_to_db(article_dict)
            database.mark_as_pulled(link)


if __name__ == '__main__':
    c = Scraper()
    c.update_sitemap_table()
    c.scrape_article_content()

