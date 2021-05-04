import requests
import threading
import os
import json
import time
from bs4 import BeautifulSoup


def links_companies():
    if os.path.exists("links_companies.json"):
        with open('links_companies.json', 'r', encoding='utf-8') as f:
            return json.load(f)

    list_links = ListLinksCompanies(
        LinksCategories().get_list()
    ).get_list()

    with open('links_companies.json', 'w') as f:
        f.write(json.dumps(list_links))

    return list_links


class LinksCategories:
    def __init__(self):
        self.categories = []
        self.collection_categories()

    def collection_categories(self):
        html = requests.get("https://www.avis-verifies.com/index.php?page=mod_annuaire").text
        soup = BeautifulSoup(html, "html.parser")

        for category in soup.findAll("div", "large-2 medium-4 small-6 columns sbloc_categorie"):
            category_link = category.find("a")["href"]
            self.categories.append(category_link)

    def get_list(self):
        return self.categories


class ListLinksCompanies:
    def __init__(self, links_categories):
        self.links_categories = links_categories
        self.companies = {}
        self.collection_companies()

    def handler_requests(self, url, next_url=None):
        if next_url is not None:
            html = requests.get(next_url).text
        else:
            html = requests.get(url).text

        soup = BeautifulSoup(html, "html.parser")
        category_name = soup.find("div", "sbloc_black").find("span").text.strip()
        for div_company in soup.findAll("div", "large-12 medium-12 columns p-t-20"):
            link_company = div_company.find("a")["href"]
            if category_name in self.companies:
                self.companies[category_name].append(link_company)
            else:
                self.companies[category_name] = [link_company]

        pager = soup.find("ul", "pager")
        if pager is not None:
            next_page_li = pager.find_all("li")[3]
            if next_page_li.has_attr("class") is False:
                next_url = next_page_li.find("a")["href"]
                self.handler_requests(url, next_url)

    def collection_companies(self):
        threads = []

        for link_category in self.links_categories:
            t = threading.Thread(
                target=self.handler_requests,
                args=(link_category,)
            )
            t.start()
            threads.append(t)

        for thread in threads:
            thread.join()

    def get_list(self):
        return self.companies


class InfoCompanies:
    def __init__(self, links):
        self.links = links
        self.info_companies = []
        self.threads = []

        self.parse()
        self.write_json()

    def write_json(self):
        with open('info_companies.json', 'w') as f:
            f.write(json.dumps(self.info_companies, indent=4))

    def parse(self):
        for name_category, list_links_companies in self.links.items():
            for link_company in list_links_companies:
                t = threading.Thread(
                    target=self.handler_request,
                    args=(name_category, link_company)
                )
                t.start()
                time.sleep(0.2)
                self.threads.append(t)
                self.wait_threads()
            self.wait_threads(True)
        self.wait_threads(True)

    def wait_threads(self, last=False):
        if last is False:
            if len(self.threads) == 40:
                for thread in self.threads:
                    thread.join()
                self.threads = []
        else:
            for thread in self.threads:
                thread.join()
            self.threads = []

    def handler_request(self, name_category, link_company):
        response = requests.get(link_company)
        if len(response.history) == 0:
            html = response.text
            soup = BeautifulSoup(html, "html.parser")
            print(link_company)
            average = self.get_average(soup)
            procent_reviews = self.get_procents_reviews(soup)
            count_all_reviews = self.get_count_all_reviews(soup)
            last_date_review = self.get_last_date_review(soup)
            info = self.get_info(soup)

            self.info_companies.append({
                "site": link_company.split("/")[-1],
                "category": name_category,
                "average": average[0],
                "average_best": average[1],
                **procent_reviews,
                "count_all_reviews": count_all_reviews,
                "last_date_review": last_date_review,
                **info
            })

    @staticmethod
    def get_average(soup):
        average = soup.find("span", "average")
        if average is None:
            return "Не указано"
        average_best = f"{average.text}/10"
        return [average.text, average_best]

    @staticmethod
    def get_procents_reviews(soup):
        if soup.find("div", "text-rate-5") is None:
            return {
                "green % reviews": "Не указано",
                "green QTY reviews": "Не указано",
                "orange % reviews": "Не указано",
                "orange QTY reviews": "Не указано",
                "red % reviews": "Не указано",
                "red QTY reviews": "Не указано"
            }
        return {
            "green % reviews": soup.find("div", "text-rate-5").findAll("div")[0].text,
            "green QTY reviews": soup.find("div", "text-rate-5").find_all("div")[1].text.split(" avis")[0],
            "orange % reviews": soup.find("div", "text-rate-3").findAll("div")[0].text,
            "orange QTY reviews": soup.find("div", "text-rate-3").find_all("div")[1].text.split(" avis")[0],
            "red % reviews": soup.find("div", "text-rate-1").findAll("div")[0].text,
            "red QTY reviews": soup.find("div", "text-rate-1").find_all("div")[1].text.split(" avis")[0]
        }

    @staticmethod
    def get_count_all_reviews(soup):
        legend = soup.find("p", "legend")
        if legend is None:
            return "Не указано"
        return legend.find("span").text.split(" avis")[0].split("* ")[1]

    @staticmethod
    def get_last_date_review(soup):
        legend = soup.find("p", "legend")
        if legend is None:
            return "Не указано"
        if legend.find("span").text.find("/") != -1:
            return legend.find("span").text.split(" ")[-1]
        return "Не указано"

    @staticmethod
    def get_info(soup):
        ignore_info = [
            "Recommander"
        ]
        info = {}

        div_informations = soup.findAll("div", "panel panel-default section informations")[-1]
        for row_info in div_informations.find("div", "panel-body").findAll("div", "row"):
            key = row_info.find("div", "info").text.strip()[:-1][:-1]
            value = row_info.find("div", "value").text.strip()
            if key not in ignore_info:
                info[key] = value

        return info


class ReviewsCompanies:
    def __init__(self, links):
        self.links = links
        self.reviews = []

        self.parse()

    def write_json(self, link):
        domain_company = link.split("/")[-1]
        with open(f"reviews/{domain_company}.json", 'w') as f:
            f.write(json.dumps(self.reviews, indent=4))
        self.reviews = []

    def parse(self):
        for name_category, links_company in self.links.items():
            for link in links_company:
                print(link)
                self.handler_page(link)
                self.write_json(link)

    def handler_page(self, url, next_url=None):
        if next_url is None:
            response = requests.get(url)
        else:
            response = requests.get(next_url)

        soup = BeautifulSoup(response.text, "html.parser")
        domain_company = url.split("/")[-1]
        div_reviews = soup.find("div", "panel panel-default section reviews")
        if div_reviews is not None:
            for div_comment in div_reviews.findAll("div", "comment"):
                stars = self.get_average(div_comment)
                comment = self.get_content_comment(div_comment)
                date = self.get_date(div_comment)
                name = self.get_name(div_comment)
                experience = self.get_experience(div_comment)

                self.reviews.append({
                    "domain": domain_company,
                    "url": url,
                    "next_url": next_url,
                    "stars": stars,
                    "comment": comment,
                    "date": date,
                    "name": name,
                    "experience": experience,
                })

        if next_url is None:
            threads = []
            pager = soup.find("ul", "pager")
            if pager is not None:
                last_page = int(pager.find("li", "current").findAll("strong")[-1].text[2:]) + 1
                if last_page != 2:
                    for i in range(1, last_page):
                        next_url = f"https://www.avis-verifies.com/avis-clients/{domain_company}?filtre=&p={i}"
                        t = threading.Thread(
                            target=self.handler_page,
                            args=(url, next_url)
                        )
                        t.start()
                        threads.append(t)

                        if len(threads) == 40:
                            for thread in threads:
                                thread.join()
                    for thread in threads:
                        thread.join()

    @staticmethod
    def get_average(div_comment):
        return div_comment.find("span", {"itemprop": "ratingValue"}).text

    @staticmethod
    def get_content_comment(div_comment):
        return div_comment.find("div", {"itemprop": "reviewBody"}).text.strip()

    @staticmethod
    def get_date(div_comment):
        return div_comment.find("meta", {"itemprop": "datePublished"})["content"]

    @staticmethod
    def get_name(div_comment):
        return div_comment.find("span", {"itemprop": "author"}).find("span").text.strip()

    @staticmethod
    def get_experience(div_comment):
        return div_comment.find("meta", {"itemprop": "dateCreated"})["content"]


#InfoCompanies(links_companies())
ReviewsCompanies(links_companies())
