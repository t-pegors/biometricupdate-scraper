# Biometricupdate.com Article Scraper

Biometric.com is a content rich website that posts daily articles about biometric technology, policy and implementation from around the world. It's a great source of content for NLP analyses.

<img src="/static/biometricupdate_home.png" alt="homepage" width="500"/>

This tool uses two primary methods to index and store article content into an sqlite3 database.  

### Update the Sitemap Tables

`c.update_sitemap_table()`

This method finds all of the relevant sitemap urls and mines them for article links. A small table (sitemap_status) records each sitemap url and whether or not it has hit the 
2000 link limit and is "completed." If not, this means links are still being daily added to the sitemap and it should be checked on next update.

<img src="/static/sitemap_status.png" alt="homepage" width="500"/>

In a separate table (sitemap_links), all article urls are stored, along with their corresponding sitemap url, date written, and whether or not the article has been pulled and 
stored by the scraper.

<img src="/static/sitemap_links.png" alt="homepage" width="600"/>

### Scrape the Articles

`scrape_article_content(limit=100)`

As of today (July 2021) there are around 18k articles total on the site starting back in 2015. Currently the scraper, will start with the oldest articles and pull the number set by the argument 'limit.'  An argument will be added to pull by year or year/month.

Raw article content is stored in a the raw_articles table. All of the content fields are pulled directly from a json extracted from the html of the article url:

<img src="/static/raw_articles.png" alt="homepage" width="800"/>
