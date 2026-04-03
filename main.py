from parser.links_from_sitemap import GetLinksSitemap
from utils.db import DbSchema
from parser.get_page_content import ThreadsPageContent


def main():
    # DbSchema().ensure_airbnb_table()

    # 1. получаем ссылки с Sitemap
    # GetLinksSitemap().run()

    # countries = ['Croatia', 'Spain', 'Germany', 'Portugal', 'France', 'Italy', 'Greece', 'Austria']

    # # 2. получаем контент со страницы
    ThreadsPageContent().run()

    # 3. создаем эксель файл 
    # Generete().run(country)

    
if __name__ == '__main__':
    main()