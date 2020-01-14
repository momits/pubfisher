import abc
from typing import TypeVar, Union, Iterator
from urllib.parse import urlparse

import re

import bibtexparser
from bs4 import BeautifulSoup
import requests
from requests.cookies import RequestsCookieJar
from requests.utils import quote
from typeguard import typechecked

from pubfisher.core import Publication, Document
from pubfisher.fishers.share import FishingError, CaptchaRequiredError, \
    PagewiseFisher, UserAbandonedCaptchaException

import gi
gi.require_version('Gtk', '3.0')
gi.require_version('WebKit2', '4.0')
from gi.repository import Gtk, WebKit2, Soup, GLib, GObject

_DEFAULT_HOST = 'https://scholar.google.com'


X = TypeVar('X')


class GSFisher(PagewiseFisher[X], abc.ABC):
    """
    Abstract base class for fishers from Google Scholar.
    """

    @typechecked
    def __init__(self, host: str=_DEFAULT_HOST, *args, **kwargs):
        super().__init__(host, *args, **kwargs)

        # If the fisher was suspended by a captcha and the user solved it,
        # GS creates new session cookies (NID, GSP) after that, which must be
        # given to this fisher to continue fishing.
        # The following flag is `True` when the needed new session cookies
        # have not yet been set.
        self._new_cookies_needed = False
        self.connect('notify::query-url', self._on_query_url_changed)
        self.connect('notify::cookies', self._on_cookies_changed)

    @GObject.Property(type=bool, default=False)  # read-only
    def new_cookies_needed(self):
        return self._new_cookies_needed

    def _on_query_url_changed(self, *args):
        self._new_cookies_needed = False
        self.notify('new-cookies-needed')

    def _on_cookies_changed(self, *args):
        if self.new_cookies_needed \
                and {'NID', 'GSP'}.issubset(set(self.cookies.keys())):
            self._new_cookies_needed = False
            self.notify('new-cookies-needed')

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

    @typechecked
    def fish_next_page(self, page_soup: BeautifulSoup=None, start_at: int=0) \
            -> Iterator[X]:
        assert not self.new_cookies_needed
        return super().fish_next_page()

    def _soup_contains_captcha(self, page_soup):
        return page_soup.find(id='gs_captcha_ccl') is not None

    def _do_fish(self, session: requests.Session, page_soup: BeautifulSoup,
                 start_at: int) -> Iterator[X]:
        if self._soup_contains_captcha(page_soup):
            self._new_cookies_needed = True
            self.notify('new-cookies-needed')
            raise FishingError(CaptchaRequiredError(),
                               self.next_url,
                               page_soup,
                               0,
                               self.user_agent,
                               session.cookies)
        else:
            return self._do_gs_fish(session, page_soup, start_at)

    @abc.abstractmethod
    def _do_gs_fish(self, session: requests.Session, page_soup: BeautifulSoup,
                    start_at: int) -> Iterator[X]:
        pass

    def _resolve_fishing_error(self, e: FishingError) -> BeautifulSoup:
        if isinstance(e.cause, CaptchaRequiredError):
            unlocked_html, new_session_cookies = _solve_captcha(e)
            # use the just unlocked html, instead of reloading the same page
            self.cookies = new_session_cookies
            return BeautifulSoup(unlocked_html, 'html.parser')
        else:
            # retry with the same soup
            return e.failure_soup


class GSCaptchaSolverWebView(WebKit2.WebView):
    """
    A *WebKit2.WebView* that can be used to solve a captcha
    that occurred in a Google Scholar query.

    The *captcha-solved* signal is emitted as soon as Google Scholar
    reloads the results page.

    The *captcha-abandoned* signal is emitted if this web view
    is destroyed before the captcha is solved.
    """

    __gsignals__ = {
        'captcha-solved': (GObject.SignalFlags.RUN_FIRST, None, (str,)),
        'captcha-abandoned': (GObject.SignalFlags.RUN_FIRST, None, ())
    }

    def __init__(self, failure_url: str, captcha_html: str, user_agent: str,
                 session_cookies: [Soup.Cookie]):
        """
        :param failure_url: The URL of the page that was blocked by the captcha
        :param captcha_html: The HTML of the captcha page that was displayed
            instead
        :param user_agent: The user agent used when the captcha occurred
        :param session_cookies: The GS session cookies used when the captcha
            occurred
        """
        super(GSCaptchaSolverWebView, self).__init__()

        parsed_uri = urlparse(failure_url)
        self.host = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        self.failure_url = failure_url
        self.captcha_html = captcha_html
        self.session_cookies = session_cookies
        self.user_agent = user_agent
        self.unlocked_html = None

        settings = self.get_settings()
        settings.enable_javascript = True
        settings.user_agent = self.user_agent

        ctx = self.get_context()
        cookie_manager = ctx.get_cookie_manager()
        cookie_manager.connect('changed', self._on_cookie_changed)
        for cookie in session_cookies:
            cookie_manager.add_cookie(cookie)

        self.connect('submit-form', self._on_submit_form)
        self.load_html(self.captcha_html, self.failure_url)

    @GObject.Property(type=bool, default=False)
    def is_captcha_solved(self):
        return self.unlocked_html is not None

    def _on_destroy(self, *args):
        if not self.is_captcha_solved:
            self.emit('captcha-abandoned')

    def _on_cookie_changed(self, cookie_manager):
        cookie_manager.get_cookies(self.host, None, self._on_save_cookies)

    def _on_save_cookies(self, cookie_manager, result):
        self.session_cookies = cookie_manager.get_cookies_finish(result)

    def _on_save_html(self, web_resource, result):
        html = web_resource.get_data_finish(result).decode('utf-8')
        self.unlocked_html = html
        self.emit('captcha-solved', html)

    def _on_results_page_loaded(self, web_view, load_event):
        if load_event == WebKit2.LoadEvent.FINISHED:
            self.get_main_resource().get_data(None, self._on_save_html)
            cookie_manager = self.get_context().get_cookie_manager()
            cookie_manager.get_cookies(self.host, None, self._on_save_cookies)

    def _on_submit_form(self, web_view, request):
        self.connect('load-changed', self._on_results_page_loaded)
        request.submit()


def _on_captcha_solved(web_view, unlocked_html, window):
    window.destroy()


def _on_window_destroy(*args):
    Gtk.main_quit()


def _solve_captcha(e: FishingError):
    window = Gtk.Window()
    window.set_title("Solve Captcha")
    window.connect("destroy", _on_window_destroy)

    ls_cookies = [Soup.Cookie.new(bs_cookie.name,
                                  bs_cookie.value,
                                  bs_cookie.domain,
                                  bs_cookie.path,
                                  bs_cookie.expires)
                  for bs_cookie in e.cookies]

    web_view = GSCaptchaSolverWebView(e.continuation_url,
                                      str(e.failure_soup),
                                      e.user_agent,
                                      ls_cookies)
    web_view.connect('captcha-solved', _on_captcha_solved, window)
    window.add(web_view)
    window.show_all()
    Gtk.main()

    if not web_view.is_captcha_solved:
        raise UserAbandonedCaptchaException()

    bs_cookies = RequestsCookieJar()
    for ls_cookie in web_view.session_cookies:
        bs_cookies.set(name=ls_cookie.get_name(),
                       value=ls_cookie.get_value(),
                       domain=ls_cookie.get_domain(),
                       path=ls_cookie.get_path(),
                       expires=ls_cookie.get_expires().to_time_t())

    return web_view.unlocked_html, bs_cookies


class PublicationGSFisher(GSFisher[Publication]):
    """
    This fisher allows to retrieve publications from Google Scholar.
    Use the *look_for_...* methods to determine which publications
    to retrieve.

    Then use the *fish_next_page* or *fish_all* methods to
    iterate over the results.
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

    def _do_gs_fish(self, session: requests.Session, page_soup: BeautifulSoup,
                    start_at: int = 0) -> Iterator[Publication]:
        outer_rows = page_soup.find_all('div', 'gs_or')[start_at:]

        for result_no, outer_row_soup in enumerate(outer_rows, start=start_at):
            try:

                inner_row_soup = outer_row_soup.find('div', class_='gs_ri')

                if inner_row_soup.find('span', class_='gs_ct1',
                                       text='[CITATION]'):
                    # The row is a citation without an abstract
                    # and without a link to an e-print. Skip that.
                    continue

                # a string used by GS to index the contained publication
                gs_info_id = outer_row_soup['data-cid']

                # a string used by GS to index the result rows
                gs_row_id = int(outer_row_soup['data-rp'])

                yield self._publication_from_row(session, inner_row_soup,
                                                 gs_info_id, gs_row_id)
            except Exception as e:
                raise FishingError(e, self.query_url, page_soup,
                                   result_no, self.user_agent,
                                   session.cookies)

    @typechecked
    def look_for_key_words(self, keywords: str):
        """
        Makes this fisher look for publications matching *keywords*.
        """
        query_url = self._GS_KEYWORD_QUERY.format(quote(keywords))
        self._set_query_url(self._absolute_from_relative_url(query_url))

    @typechecked
    def look_for_citations_of(self, doc: Union[Document, str]):
        """
        Makes this fisher look for publications citing *doc*.
        *doc* may be a *Document* or a GS id for a document.
        """
        if isinstance(doc, Document):
            gs_doc_id = doc.gs_doc_id
        else:
            gs_doc_id = doc

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

    @typechecked
    def download_e_print(self, pub: Publication, path: str):
        """
        Downloads the e-print of *pub* to *path* and updates *pub* to
        reference the downloaded e-print.
        """
        assert pub.eprint

        with open(path, 'wb') as f:
            with self._create_http_session() as session:
                resp = session.get(pub.eprint)
                f.write(resp.content)

        pub.eprint = path
