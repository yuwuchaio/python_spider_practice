import requests
from bs4 import BeautifulSoup
import codecs

DOWNLOAD_URL = 'http://movie.douban.com/top250'
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36'}

def download_page(url):
    data = requests.get(url, headers=headers).content
    return data

def getTotalPage(html):
    pass


def parser_html(html):
    movie_name_list = []

    bs = BeautifulSoup(html, features='html.parser')
    movie_list_soup = bs.find('ol', attrs={'class': 'grid_view'})
    for movie_li in movie_list_soup.find_all('li'):

        detail = movie_li.find('div', attrs={'class': 'hd'})
        movie_names = detail.find_all('span', attrs={'class': 'title'})
        other_name = detail.find('span', attrs={'class': 'other'})
        if other_name:
            movie_names.append(other_name)
        movie_names = [name.getText() for name in movie_names]
        totalName = ' '.join(movie_names)
        totalName = totalName

        movie_name_list.append(totalName)


    next_page = bs.find('span', attrs={'class': 'next'}).find('a')
    if next_page:
        return movie_name_list, DOWNLOAD_URL + next_page['href']
    return movie_name_list, None


def main():
    url = DOWNLOAD_URL
    with codecs.open('movies', 'wb', encoding='utf-8') as fp:
        while url:
            html = download_page(url)
            movies, url = parser_html(html)
            fp.write(u'{movies}\n'.format(movies='\n'.join(movies)))

    print(movies)


if __name__ == '__main__':
    main()

