import abc
from contextlib import contextmanager
from typing import TypeVar, Generic, Iterable, Optional

from gi.repository import GObject

import re

import bibtexparser
from bs4 import BeautifulSoup
import requests
from requests.cookies import RequestsCookieJar
from requests.utils import quote
from typeguard import typechecked

from pubfisher.core import Publication, Document

_DEFAULT_HOST = 'https://scholar.google.com'


class ScrapeError(Exception):
    def __init__(self, cause: Exception, continuation_url: str,
                 failure_soup: BeautifulSoup, result_no: int,
                 user_agent: str, cookies: RequestsCookieJar):
        """
        :param cause: The Exception that occurred during scraping
        :param continuation_url: URL of the page that could not be scraped
        :param failure_soup: `BeautifulSoup` representing the HTML page that
            could not be scraped
        :param user_agent: The user agent string used when the exception
            occurred
        :param cookies: A `RequestsCookieJar` containing the Google Scholar
            session cookies used when the exception occurred
        """
        self.cause = cause
        self.continuation_url = continuation_url
        self.failure_soup = failure_soup
        self.result_no = result_no
        self.user_agent = user_agent
        self.cookies = cookies

    def __str__(self):
        return 'Error retrieving result no {} from {}: {}' \
            .format(self.result_no, self.continuation_url, str(self.cause))


class CaptchaRequiredError(Exception):
    """
    Exception that is raised when GS requires to solve a captcha
    in order to retrieve more results.

    If desired, the user can solve the captcha by looking at the *captcha_soup*,
    and then send the result to GS using *user_agent* and *cookies* to continue
    the HTTP session.
    """


class HTTPError(Exception):
    def __init__(self, code: int, msg: str):
        self.code = code
        self.msg = msg

    def __str__(self):
        return 'Error {}: {}'.format(self.code, self.msg)


def _soup_contains_captcha(soup):
    return soup.find(id='gs_captcha_ccl') is not None


X = TypeVar('X')


class _Meta(type(GObject.GObject), type(abc.ABC)):
    pass


class GScraper(GObject.GObject, Generic[X], abc.ABC, metaclass=_Meta):
    """
    Abstract base class for scrapers from Google Scholar.
    """

    host = GObject.Property(type=str)

    @typechecked
    def __init__(self, host: str=_DEFAULT_HOST, user_agent: Optional[str]=None,
                 cookies: Optional[RequestsCookieJar]=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Whether there are result pages for the query which have not yet
        # been scraped.
        self._has_next_page = True

        # The url where the current query started.
        self._query_url = None
        # The url we will scrape next.
        self._next_url = None

        # The GS host we are scraping from. The default host should be fine.
        self.host = host

        # Custom headers + cookies allow to appear as any specific browser
        # while communicating with GS.
        # The cookies will be set for each request,
        # so that e.g. the GS session key cookies (GSP and NID) can be used
        # for a seamless handover from another browser.
        # Whenever a HTTP session terminates, all of its set cookies are stored
        # by the scraper for the next session to be reused.
        self._user_agent = user_agent
        self._cookies = cookies  # should be a RequestsCookieJar or None

        # If the scraper was suspended by a captcha and the user solved it,
        # GS creates new session cookies (NID, GSP) after that, which must be
        # given to this scraper for the scraping to continue.
        # The following flag is `True` when the needed new session cookies
        # have not yet been set.
        self._new_cookies_needed = False

        self.connect('notify::query-url', self._on_query_url_changed)

    def _absolute_from_relative_url(self, rel_url):
        return self.host + rel_url + '&hl=en'

    @GObject.Property(type=str)
    def user_agent(self):
        return self._user_agent

    @user_agent.setter
    def user_agent(self, user_agent):
        self._user_agent = user_agent
        self.notify('headers')  # 'headers' depends on 'user-agent'

    @GObject.Property(type=str)  # read-only
    def query_url(self):
        """
        The URL representing the start of the last GS query.
        """
        return self._query_url

    def _on_query_url_changed(self, *args):
        self._new_cookies_needed = False
        self.notify('new-cookies-needed')

    @GObject.Property(type=str)  # read-only
    def next_url(self):
        """
        The URL of the GS results page that will be scraped next.
        """
        return self._next_url

    @GObject.Property(type=bool, default=False)  # read-only
    def new_cookies_needed(self):
        return self._new_cookies_needed

    @GObject.Property(type=bool, default=True)  # read-only
    def has_next_page(self):
        """
        Whether there are more result pages of the current query which
        can be scraped.
        """
        return self._next_url is not None

    @GObject.Property
    def cookies(self):
        """
        The cookies this scraper currently sends to GS upon requests.
        """
        return self._cookies

    @cookies.setter
    @typechecked
    def cookies(self, cookies: RequestsCookieJar):
        """
        Set the cookies this scraper sends to GS upon requests.
        Can be used to make the scraper take over another GS session.
        If *cookies* contains the GS session cookies (NID and GSP),
        the 'new-cookies-needed' property is set to `False`.
        """
        if self.new_cookies_needed \
                and {'NID', 'GSP'}.issubset(set(cookies.keys())):
            self._new_cookies_needed = False
            self.notify('new-cookies-needed')
        self._cookies = cookies

    @GObject.Property  # read-only
    def headers(self):
        """
        The HTTP headers this scraper currently sends to GS upon requests.
        """
        headers = {'Accepted-Language': 'en-US'}
        if self.user_agent:
            headers['User-Agent'] = self.user_agent
        return headers

    def _get_page(self, session, url):
        """
        Perform a GET request on URL and return the response data.
        """
        resp = session.get(url, headers=self.headers)
        if resp.status_code == 200:
            return resp.text
        else:
            raise HTTPError(resp.status_code, resp.reason)

    def _get_soup_from_url(self, session, url):
        """
        Load the HTML page located at *url*
        and turn it into a BeautifulSoup.
        """
        html = self._get_page(session, url)
        html = html.replace(u'\xa0', u' ')
        return BeautifulSoup(html, 'html.parser')

    @contextmanager
    def _create_http_session(self):
        session = requests.Session()
        if self.cookies:
            session.cookies = self.cookies
        try:
            yield session
        except:
            # some request failed in the session.
            # we want to save the session cookies.
            self._cookies = session.cookies
            self.notify('cookies')
            raise
        finally:
            session.close()

    def _find_next_url(self, page_soup: BeautifulSoup) -> str:
        """
        Finds the URL to the next results page using the *page_soup* of
        the current result page. Returns *None* if there is no such URL.
        May be overridden in subclasses.

        :param page_soup: the page to find the url on
        :return: the url to the next results page
        """
        if page_soup.find(class_='gs_ico gs_ico_nav_next'):
            next_page_rel_url = page_soup.find(class_='gs_ico '
                                                      'gs_ico_nav_next') \
                                         .parent['href']
            return self._absolute_from_relative_url(next_page_rel_url)

    def _set_query_url(self, query_url: str):
        """
        Sets the 'next-url' property to a new *next_url* and emits the
        necessary *notify*-signals.
        Should only be called privately in subclasses.
        """
        self._query_url = query_url
        self._next_url = query_url
        self.notify('query-url')
        self.notify('next-url')
        self.notify('has-next-page')

    @typechecked
    def scrape_next_page(self, page_soup: BeautifulSoup=None, start_at: int=0) \
            -> Iterable[X]:
        """
        Scrape all results from the page represented by *soup* and the
        result position on this page represented by *start_at*.
        If *soup* is None, scrape from the result page that follows
        the previously scraped page. If this previous page was the
        last page of the Google Scholar results a `StopIteration`
        exception is raised.
        """
        if not self.has_next_page:
            raise StopIteration()

        assert not self.new_cookies_needed

        with self._create_http_session() as session:
            if page_soup is None:
                page_soup = self._get_soup_from_url(session, self.query_url)

            if _soup_contains_captcha(page_soup):
                self._new_cookies_needed = True
                self.notify('new-cookies-needed')
                raise ScrapeError(CaptchaRequiredError(),
                                  self.next_url,
                                  page_soup,
                                  0,
                                  self.user_agent,
                                  session.cookies)

            else:
                yield from self._do_scrape(session, page_soup, start_at)

            self._next_url = self._find_next_url(page_soup)
            self.notify('next-url')
            if self._next_url is None:
                self.notify('has-next-page')

    @abc.abstractmethod
    def _do_scrape(self, session: requests.Session, page_soup: BeautifulSoup,
                   start_at: int) -> Iterable[X]:
        """
        Scrape result objects from a GS result page given by *soup*.

        :param session: The HTTP session to be used for making additional
            queries (if desired)
        :param page_soup: The `BeautifulSoup` representing the current
            GS result page
        :param start_at: The index of the result on the result page where
            scraping must begin
        :return: the result objects
        """
        pass


class PublicationGScraper(GScraper[Publication]):
    """
    This iterable allows to iterate over all publications that are in the
    results of a Google Scholar query.
    Each iterator produced by this iterable yields the results of
    one page of the Google Scholar results.
    The whole result set can be queried by iterating over each
    of the page-wise iterators.
    To avoid overloading the google servers there should be a delay
    between querying the individual publications on a search page,
    as well as between the different search pages.

    If a captcha is required before the results are exhausted
    (which is usually the case) a `CaptchaRequiredException` is raised.
    The user can use the session data provided with the exception
    to solve the captcha and then call this iterable again.
    It will continue scraping the results starting at the URL
    that had previously failed.
    """

    _GS_CITATION_ID_RE = r'cites=([\w-]*)'
    _GS_YEAR_RE = r'(?P<year>\d{4})\s-'

    _GS_KEYWORD_QUERY = '/scholar?q={0}'
    _GS_CITES_QUERY = '/scholar?&cites={0}'
    _GS_CITING_INFO_QUERY = '/scholar?q=info:{0}:scholar.google.com/' \
                            '&output=cite&scirp={1}'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _find_title_heading(self, row):
        title_heading = row.find('h3', class_='gs_rt')
        if title_heading.find('span', class_='gs_ctu'):
            title_heading.span.extract()  # Rip out a citation mark
        elif title_heading.find('span', class_='gs_ctc'):
            title_heading.span.extract()  # Rip out a book or PDF mark
        return title_heading

    def _find_title_and_url(self, row_soup):
        title_heading = self._find_title_heading(row_soup)
        title = title_heading.text.strip()
        link = title_heading.find('a')
        url = link['href'] if link else None
        return title, url

    def _find_author_box(self, row_soup):
        return row_soup.find('div', class_='gs_a')

    def _find_authors_and_year(self, row_soup):
        author_box = self._find_author_box(row_soup)
        authors, rest = author_box.text.split(' - ', 1)
        year = None
        try:
            year = int(next(re.finditer(self._GS_YEAR_RE, rest)).group('year'))
        except StopIteration:
            pass
        finally:
            return authors, year

    def _find_abstract(self, row_soup):
        abstract = row_soup.find('div', class_='gs_rs').text
        if abstract[0:8].lower() == 'abstract':
            return abstract[9:].strip()
        else:
            return abstract

    def _find_lower_links(self, row_soup):
        return row_soup.find('div', class_='gs_fl').find_all('a')

    def _find_e_print(self, row_soup):
        e_print_box = row_soup.find('div', class_='gs_ggs gs_fl')
        if e_print_box:
            return e_print_box.a['href']

    def _find_citation_count_and_id(self, row_soup):
        lower_links = self._find_lower_links(row_soup)
        citation_count = None
        citation_id = None

        for link in lower_links:
            if 'Cited by' in link.text:
                citation_count = int(re.findall(r'\d+', link.text)[0])
                citation_id = re.findall(self._GS_CITATION_ID_RE,
                                         link['href'])[0]

        return citation_count, citation_id

    def _publication_from_row(self, session, row_soup, gs_info_id, row_id):
        authors, year = self._find_authors_and_year(row_soup)
        title, url = self._find_title_and_url(row_soup)

        abstract = self._find_abstract(row_soup)
        citation_count, gs_doc_id \
            = self._find_citation_count_and_id(row_soup)
        e_print = self._find_e_print(row_soup)

        document = Document(title, authors, abstract, citation_count, gs_doc_id)
        return Publication(document, year, url, e_print, gs_info_id,
                           None)

    def _do_scrape(self, session: requests.Session, page_soup: BeautifulSoup,
                   start_at: int = 0) -> Iterable[Publication]:
        outer_rows = page_soup.find_all('div', 'gs_or')[start_at:]

        for result_no, outer_row_soup in enumerate(outer_rows, start=start_at):
            try:

                inner_row_soup = outer_row_soup.find('div', class_='gs_ri')

                # a string used by GS to index the contained publication
                gs_info_id = outer_row_soup['data-cid']

                # a string used by GS to index the result rows
                gs_row_id = int(outer_row_soup['data-rp'])

                yield self._publication_from_row(session, inner_row_soup,
                                                 gs_info_id, gs_row_id)
            except Exception as e:
                raise ScrapeError(e, self.query_url, page_soup,
                                  result_no, self.user_agent,
                                  session.cookies)

    @typechecked
    def query_matches_of_key_words(self, keywords: str):
        """
        Makes this scraper query publications matching *keywords*.
        """
        query_url = self._GS_KEYWORD_QUERY.format(quote(keywords))
        self._set_query_url(self._absolute_from_relative_url(query_url))

    @typechecked
    def query_citations_of_document(self, doc: Document):
        """
        Makes this scraper query publications citing *doc*.
        """
        self.query_citations_of_document_id(doc.gs_doc_id)

    @typechecked
    def query_citations_of_document_id(self, gs_doc_id: str):
        """
        Makes this scraper query publications citing the document
        identified by *gs_doc_id*.
        """
        query_url = self._GS_CITES_QUERY.format(quote(gs_doc_id))
        self._set_query_url(self._absolute_from_relative_url(query_url))

    def _get_bibtex(self, session, gs_info_id, gs_row_id):
        rel_info_url = self._GS_CITING_INFO_QUERY.format(gs_info_id, gs_row_id)
        info_url = self._absolute_from_relative_url(rel_info_url)

        citation_info_soup = self._get_soup_from_url(session, info_url)
        bibtex_link = citation_info_soup.find('a', string='BibTeX')
        if bibtex_link:
            return self._get_page(session, bibtex_link['href'])

    @typechecked
    def update_bibtex(self, pub: Publication, update_authors: bool=True,
                      update_year: bool=True):
        """
        Retrieves and stores the BibTex entry of *pub* according to GS.
        If *update_authors* is `True`, the authors property of *pub*
        is updated using the corresponding BibTex field.
        Analogously for *update_year*.
        """
        with self._create_http_session() as session:
            bibtex_plain = self._get_bibtex(session, pub.gs_pub_id, 0)
            if bibtex_plain:
                bibtex = bibtexparser.loads(bibtex_plain).entries[0]
                pub.bibtex = bibtex
                if update_authors:
                    pub.document.authors = bibtex['author']
                if update_year:
                    pub.document.year = int(bibtex['year'])


# class AuthorGScraper(GScraper):
#     """
#     This iterable returns all authors that are in the
#     results of a Google Scholar query.
#     """
#
#     def __init__(self, scholar_query, *args, **kwargs):
#         super(AuthorGScraper, self).__init__(scholar_query, *args, **kwargs)
#
#     def __iter__(self):
#         with requests.Session() as session:
#             assert not self.new_cookies_needed
#             session.cookies = self._cookies
#
#             soup = self._get_soup_from_url(session, self._current_url)
#
#             while True:
#                 for row in soup.find_all('div', 'gsc_1usr'):
#                     yield Author(row)
#                 next_button = soup.find(class_='gs_btnPR gs_in_ib gs_btn_half gs_btn_lsb gs_btn_srt gsc_pgn_pnx')
#                 if next_button and 'disabled' not in next_button.attrs:
#                     url = next_button['onclick'][17:-1]
#                     url = codecs.getdecoder("unicode_escape")(url)[0]
#                     soup = self._get_soup_from_url(session, url)
#                 else:
#                     break

