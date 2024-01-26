import random
import scrapy
from typing import List


class StackOverflow(scrapy.Spider):
    name = "stackoverflow"

    def start_requests(self):
        """Формируем URLs и начинаем формировать запросы"""
        urls = [
            f"https://ru.stackoverflow.com/questions/{i}/"
            for i in range(1, 1563318)
        ]
        # Перемешиваем запросы
        random.shuffle(urls)
        # Отправляем запросы
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def make_text(self, raw: List[scrapy.Selector]) -> str:
        """Составляем единый текст без HTML-кодов"""
        text = ""
        for raw_text in raw:
            code = raw_text.css("pre code::text").get()
            # Если код, то добавляем символ кода
            if code:
                text += "```\n"
            # Добавляем текст
            text += (
                code
                or raw_text.css("p::text").get()
                or raw_text.css("blockquote::text").get()
                or ""
            )
            # Аналогично
            if code:
                text += "```"
            # Перенос строки
            text += "\n"
        return text

    def parse(self, response) -> dict:
        """Парсим страницу"""
        # Проходимся по постам и составляем единый текст
        posts = []
        for post in response.css(".post-layout"):
            # Голоса за пост
            votes = post.css(".js-vote-count::text").get().strip()
            # Формируем текста поста
            text = self.make_text(post.css(".js-post-body > *"))
            # Добавляем в общий список
            posts.append({"votes": votes, "text": text})
        # Возвращаем заголовок и посты
        yield {
            "title": response.css(".question-hyperlink::text")[0].get(),
            "posts": posts,
        }
