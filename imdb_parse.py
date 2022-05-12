from requests import get
import datetime as dt
from math import ceil
import os
import multiprocessing

import pandas as pd
from bs4 import BeautifulSoup

today = dt.date.today()


# outputs a link to imdb search page of all titles released from start date till today, starting from a certain number
def make_url(start=1, start_date="1995-01-01"):
    url = 'https://www.imdb.com/search/title/?release_date={},{}&num_votes=5000,&countries=us&sort=release_date,' \
          'asc&count=250&start={}&ref_=adv_nxt'.format(start_date, today, str(start))
    return url


def find_links_amount(start_date="1995-01-01"):
    # finding out the amount of titles, that will be used further
    url = make_url(start_date=start_date)
    response = get(url)
    html_soup = BeautifulSoup(response.text, 'html.parser')
    amount_container = html_soup.find('div', class_='desc')
    amount_str = amount_container.span.text[9:15]  # looks like     1-250 of 9,643 titles.
    if amount_str[5] == " ":
        links_amount = int(amount_str[0] + amount_str[2:5])
    else:
        links_amount = int(
            amount_str[0:2] + amount_str[3:6])  # gives 4- or 5-digit number of available titles (by the time of
        # writing this program, the amount was pretty close to 10000, so this was made for the future)
    return links_amount


def get_links(num: int):
    response = get(make_url(num))
    html_soup = BeautifulSoup(response.text, 'html.parser')
    content_unit = html_soup.find_all("div", class_='lister-item-content')
    for item in content_unit:
        spans = item.find_all("span", class_='text-muted')
        textspans = []
        for j in spans:
            textspans.append(j.text)
        if "Gross:" in textspans:
            content_unit = item.find('h3', class_='lister-item-header')
            link = "".join(["http://imdb.com", content_unit.find('a', href=True)['href']])
            pd.DataFrame({"Links": [link]}).to_csv("temporary.csv", mode='a', header=False, index=False)


def get_titles_links(start_date="1995-01-01"):
    # collecting the links for movies with gross only (automatically excluding series, as they have no gross)
    links_amount = find_links_amount(start_date=start_date)
    pd.DataFrame(columns=["Links"]).to_csv("temporary.csv", index=False)
    args = []
    for i in range(ceil(links_amount / 250)):
        args.append((250 * i + 1,))
    pool = multiprocessing.Pool()
    pool.starmap(get_links, args)
    pool.close()
    print("Congrats!\nFull list of links has been successfully filtered!")


def find_simple_feature(soup: str, container_tag: str, container_attrs_key: str, container_attrs_value: str,
                        feature_tag: str, feauture_class: str,
                        is_multiple=False) -> str:
    container = soup.find(container_tag, attrs={container_attrs_key: container_attrs_value})
    if container is not None:
        if is_multiple:
            features = container.find_all(feature_tag, class_=feauture_class)
            feature = "+".join([i.text for i in features])
        else:
            feature = container.find(feature_tag, class_=feauture_class).text
    else:
        feature = None
    return feature


def convert_to_usd(amount, currency):
    currency_dict = {"£": "GBP", "₹": "INR", '€': "EUR", "THB": "THB", "DKK": "DKK", "HUF": "HUF", "CA$": "CAD",
                     "A$": "AUD", "CN¥": "CNY", "HK$": "HKD", "PLN": "PLN"}
    link_for_currencies = "https://www.xe.com/currencyconverter/convert/?Amount={}&From={}&To=USD"
    if currency in currency_dict.keys():
        usd_link = link_for_currencies.format(amount, currency_dict[currency])
        usd_response = get(usd_link)
        usd_soup = BeautifulSoup(usd_response.text, 'html.parser')
        usd_amount_string = usd_soup.find("p", class_="result__BigRate-sc-1bsijpp-1 iGrAod")
        if usd_amount_string is not None:
            usd_amount = "$"
            for i in usd_amount_string.text:
                if i.isnumeric():
                    usd_amount += i
                elif i == ".":
                    break
            return usd_amount
        else:
            return None
    elif currency == "$":
        usd_amount = "$" + "".join([i for i in amount if i.isnumeric()])
        return usd_amount
    else:
        return None


def parse_movie_link(link: str):
    response = get(link, headers={"Accept-Language": "en-US, en;q=0.5"})
    html_soup = BeautifulSoup(response.text, 'html.parser')

    # sometimes imdb has no information about one of the features we need
    # most of the features are None than, but some features like release year or gross can be tried to obtain anyway.
    # For example, if there are no worldwide gross, it captures the biggest gross in section
    title_container = html_soup.find("h1", attrs={"data-testid": "hero-title-block__title"})
    if title_container is not None:
        title = title_container.text
    else:
        title = None

    genres = find_simple_feature(html_soup, "li", "data-testid", "storyline-genres", "a",
                                 "ipc-metadata-list-item__list-content-item "
                                 "ipc-metadata-list-item__list-content-item--link", True)
    age_rating = find_simple_feature(html_soup, "li", "data-testid", "storyline-certificate", "span",
                                     "ipc-metadata-list-item__list-content-item")
    companies = find_simple_feature(html_soup, "li", "data-testid", "title-details-companies", "a",
                                    "ipc-metadata-list-item__list-content-item "
                                    "ipc-metadata-list-item__list-content-item--link", True)
    length = find_simple_feature(html_soup, "li", "data-testid", "title-techspec_runtime", "div",
                                 "ipc-metadata-list-item__content-container")

    score_container = html_soup.find("span", class_="sc-7ab21ed2-1 jGRxWM")
    if score_container is not None:
        score = score_container.text
    else:
        score = None

    num_of_scores_container = html_soup.find('div', class_='sc-7ab21ed2-3 dPVcnq')
    if num_of_scores_container is not None:
        num_of_scores = num_of_scores_container.text
    else:
        num_of_scores = None

    # other features are not that simple to get...
    authors_container = html_soup.find_all("li", attrs={"data-testid": "title-pc-principal-credit"})
    if authors_container is not None:
        if "Writer" or "Writers" in [i.text for i in
                                     authors_container.find_all("span", class_="ipc-metadata-list-item__label")]:
            if "Director" or "Directors" in [i.text for i in
                                             authors_container.find_all("span",
                                                                        class_="ipc-metadata-list-item__label")]:
                # directors is always the first section
                directors_container = authors_container[0].find_all("a",
                                                                    class_="ipc-metadata-list-item__list-content-item "
                                                                           "ipc-metadata-list-item__list-content-item"
                                                                           "--link")
                directors = "+".join([i.text for i in directors_container])

                writers_container = authors_container[1].find_all("a",
                                                                  class_="ipc-metadata-list-item__list-content-item "
                                                                         "ipc-metadata-list-item__list-content-item"
                                                                         "--link")
                writers = "+".join([i.text for i in writers_container])
            else:
                directors = None
                writers_container = authors_container[0].find_all("a",
                                                                  class_="ipc-metadata-list-item__list-content-item "
                                                                         "ipc-metadata-list-item__list-content-item"
                                                                         "--link")
                writers = "+".join([i.text for i in writers_container])
        else:
            writers = None
            if "Director" or "Directors" in [i.text for i in
                                             authors_container.find_all("span",
                                                                        class_="ipc-metadata-list-item__label")]:
                directors_container = authors_container[0].find_all("a",
                                                                    class_="ipc-metadata-list-item__list-content-item "
                                                                           "ipc-metadata-list-item__list-content-item"
                                                                           "--link")
                directors = "+".join([i.text for i in directors_container])
            else:
                directors = None
        if "Star" or "Stars" in [i.text for i in
                                 authors_container.find_all("span", class_="ipc-metadata-list-item__label")]:
            # stars are always the last section
            stars_container = authors_container[-1].find_all("a", class_="ipc-metadata-list-item__list-content-item "
                                                                         "ipc-metadata-list-item__list-content-item"
                                                                         "--link")
            stars = "+".join([i.text for i in stars_container])
        else:
            stars = None
    else:
        directors = None
        writers = None
        stars = None

    release_year_container = html_soup.find("li", attrs={"data-testid": "title-details-releasedate"})
    if release_year_container is not None:
        full_release_date = release_year_container.find("a",
                                                        class_="ipc-metadata-list-item__list-content-item "
                                                               "ipc-metadata-list-item__list-content-item--link").text
        if len(full_release_date.split(",")) == 2:
            release_yr = full_release_date.split(",")[1][1:5]
            release_date = full_release_date.split(",")[0]
        else:
            release_yr = full_release_date[0:5]
            release_date = None
    else:
        release_yr = None
        release_date = None

    worldwide_gross_container = html_soup.find("li", attrs={"data-testid": "title-boxoffice-cumulativeworldwidegross"})
    if worldwide_gross_container is not None:
        gross = worldwide_gross_container.find("span", class_="ipc-metadata-list-item__list-content-item").text
        currency = ""
        for k in gross:
            if not k.isnumeric() and k != u'\xa0':
                currency += k
            else:
                break
        gross = convert_to_usd(gross, currency)
        budget_container = html_soup.find("li", attrs={"data-testid": "title-boxoffice-budget"})
        if budget_container is not None:
            budget = budget_container.find("span", class_="ipc-metadata-list-item__list-content-item").text
            currency = ""
            for k in budget:
                if not k.isnumeric() and k != u'\xa0':
                    currency += k
                else:
                    break
            budget = "".join([i for i in budget if i.isnumeric()])
            budget = convert_to_usd(budget, currency)
        else:
            budget = None
    else:
        grosses_container = html_soup.find("div", attrs={"data-testid": "title-boxoffice-section"})
        if grosses_container is not None:
            budget_container = html_soup.find("li", attrs={"data-testid": "title-boxoffice-budget"})
            if budget_container is not None:
                budget = budget_container.find("span", class_="ipc-metadata-list-item__list-content-item").text
                currency = ""
                for k in budget:
                    if not k.isnumeric() and k != u'\xa0':
                        currency += k
                    else:
                        break
                budget = "".join([i for i in budget if i.isnumeric()])
                budget = convert_to_usd(budget, currency)
            else:
                budget = None
            all_grosses_containers = grosses_container.find_all("span",
                                                                class_="ipc-metadata-list-item__list-content-item")
            grosses_texts = [i.text if i.text != budget else "0" for i in all_grosses_containers]
            gross_dict = {}
            for j in grosses_texts:
                potential_gross = int("".join([k for k in j if k.isnumeric()]))
                currency = ""
                for k in j:
                    if not k.isnumeric() and k != u'\xa0':
                        currency += j
                    else:
                        break
                gross_dict[potential_gross] = currency
            grosses_in_usd = []
            for j in gross_dict.keys():
                usd_amount = convert_to_usd(j, gross_dict[j])
                if usd_amount is not None:
                    grosses_in_usd.append(usd_amount[1:])
            gross = "$" + str(max([grosses_in_usd]))
        else:
            gross = None
            budget = None

    link_for_keywords = "".join([link, "keywords?ref_=tt_stry_kw"])
    response = get(link_for_keywords)
    html_soup = BeautifulSoup(response.text, 'html.parser')
    keywords_table = html_soup.find("table", class_="dataTable evenWidthTable2Col")
    if keywords_table:
        keywords_list = keywords_table.find_all("div", class_="sodatext")
        keywords = "+".join([i.a.text for i in keywords_list])
    else:
        keywords = None

    print([title, release_yr, release_date, age_rating, length, genres,
           directors, writers, stars, companies, score, num_of_scores, budget, gross, keywords])
    pd.DataFrame([title, release_yr, release_date, age_rating, length, genres,
                  directors, writers, stars, keywords, companies, score, num_of_scores, budget, gross]).T.to_csv(
        "movies_data.csv", mode='a', header=False, index=False)


def parse_list_of_links(some_list: list):
    print("Starting to parse...")
    pool = multiprocessing.Pool()
    args = []
    for i in range(len(some_list)):
        args.append((some_list[i],))
    pool.starmap(parse_movie_link, args)
    pool.close()
    try:
        os.remove("temporary.csv")
    except OSError:
        pass
    print("\n\nCongratulations! Movie database was successfully created!")


# actual parsing
def main():
    print("Starting...\nPlease wait..")
    pd.DataFrame(
        columns=["Title", "ReleaseYr", "ReleaseDate", "AgeRating", "Length", "Genres", "Directors",
                 "Writers", "Stars", "Keywords", "ProductionCompanies", "Score", "NumberOfScores", "Budget",
                 "Gross"]).to_csv("movies_data.csv", index=False)
    get_titles_links()
    titles_links = list(pd.read_csv("temporary.csv").loc[:, "Links"])
    parse_list_of_links(titles_links)


if __name__ == "__main__":
    main()
