from scrapy import Spider, Request


class WikipediaSpider(Spider):
    name = "wikipedia"
    allowed_domains = ["ru.wikipedia.org"]
    start_urls = ["https://ru.wikipedia.org/w/index.php?title=%D0%9A%D0%B0%D1%82%D0%B5%D0%B3%D0%BE%D1%80%D0%B8%D1%8F%3A%D0%A4%D0%B8%D0%BB%D1%8C%D0%BC%D1%8B_%D0%BF%D0%BE_%D0%B0%D0%BB%D1%84%D0%B0%D0%B2%D0%B8%D1%82%D1%83"]

    def parse(self, response):
        all_pages = response.xpath("//*[@class='external text']/@href").getall()[1:]
        for url in all_pages:
            yield Request(
                response.urljoin(url),
                callback=self.parse_category,
            )

    def parse_category(self, response):
        films = response.xpath("//*[@class='mw-category-group']//a/@href").getall()
        for film in films:
            yield Request(
                response.urljoin(film),
                callback=self.parse_film,
            )

    def parse_film(self, response):
        countries = ",".join(response.xpath("//*[@class='country-name']//a/text()").getall())
        if not countries:
            countries = ",".join(response.xpath("//*[contains(text(),'Стран')]/ancestor::tr//span/a/@title").getall())

        year = response.xpath("//*[contains(text(),'Год')]/ancestor::tr//a/@title").re('(\d{1,})\s')
        year = (int(year[0]) if year else None)

        item = {
            "title": response.xpath("//*[@class='mw-page-title-main']/text()").get(),
            "genres": ",".join(response.xpath("//*[contains(text(),'Жанр')]/ancestor::tr//span/a/text()").getall()),
            "filmmaker": response.xpath("//*[contains(text(),'Режиссёр')]/ancestor::tr//span/a/text()").get(),
            "country": countries,
            "year": year,
        }
        yield item
