from parser.links_from_sitemap import GetLinksSitemap
from utils.db import DbSchema


def main():
    DbSchema().ensure_airbnb_table()

    # 1. получаем ссылки с Sitemap
    GetLinksSitemap().run()

    # countries = ['Croatia', 'Spain', 'Germany', 'Portugal', 'France', 'Italy', 'Greece', 'Austria']

    # country = 'Greece'

    # for country in countries:
    # # 2. получаем контент со страницы
    # ThreadsPageContent(country).run()

    # 3. создаем эксель файл 
    # Generete().run(country)

    
if __name__ == '__main__':
    main()