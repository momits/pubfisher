import abc
import random
import time
from contextlib import contextmanager
from typing import Optional, Generic, TypeVar, Iterator

import requests
from bs4 import BeautifulSoup
from requests.cookies import RequestsCookieJar
from typeguard import typechecked

from gi.repository import GObject


class FishingError(Exception):
    """
    Raised if fishing from a webservice was interrupted.

    If desired, the user can resolve the error by looking at the *failure_soup*
    and communicating with the webservice using *user_agent* and *cookies*.
    """

    def __init__(self, cause: Exception, continuation_url: str,
                 failure_soup: BeautifulSoup, result_no: int,
                 user_agent: str, cookies: RequestsCookieJar):
        """
        :param cause: The Exception that occurred during fishing
        :param continuation_url: URL of the page that could not be fished
        :param failure_soup: `BeautifulSoup` representing the HTML page that
            could not be fished
        :param user_agent: The user agent string used when the exception
            occurred
        :param cookies: A `RequestsCookieJar` containing the
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
    Raised when a webservice requires to solve a captcha
    in order to retrieve more results.
    """


class UserAbandonedCaptchaException(Exception):
    """
    Raised when a user resigns solving a captcha.
    """


class HTTPError(Exception):
    """
    Raised when a HTTP status code different from 200 is received
    that could not be dealt with.
    """

    def __init__(self, code: int, msg: str):
        self.code = code
        self.msg = msg

    def __str__(self):
        return 'Error {}: {}'.format(self.code, self.msg)


X = TypeVar('X')


class _Meta(type(GObject.GObject), type(abc.ABC)):
    pass


class PagewiseFisher(GObject.GObject, Generic[X], abc.ABC, metaclass=_Meta):
    """
    Abstract base class for fishers who fish from the result pages of a search
    engine.
    """

    host = GObject.Property(type=str)

    @typechecked
    def __init__(self, host: str, user_agent: Optional[str] = None,
                 cookies: Optional[RequestsCookieJar] = None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Whether there are result pages for the query which have not yet
        # been fished.
        self._has_next_page = True

        # The url where the current query started.
        self._query_url = None
        # The url we will fish in next.
        self._next_url = None

        # The host we are fishing from. The default host should be fine.
        self.host = host

        # Custom headers + cookies allow to appear as any specific browser
        # while communicating with the web service.
        # They can be set any time, e.g. for a seamless handover from another
        # browser.
        # Whenever a HTTP session terminates, all of its set cookies are stored
        # by the fisher for the next session to be reused.
        self._user_agent = user_agent
        self._cookies = cookies

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
        The URL pointing to the first result page of the query we are currently
        fishing from.

        This property is read-only and can only be manipulated
        by calling a *look_for_...* method.
        """
        return self._query_url

    @GObject.Property(type=str)  # read-only
    def next_url(self):
        """
        The URL of the results page that will be fished next.
        """
        return self._next_url

    @GObject.Property(type=bool, default=False)  # read-only
    def new_cookies_needed(self):
        return self._new_cookies_needed

    @GObject.Property(type=bool, default=True)  # read-only
    def has_next_page(self):
        """
        Whether there are more result pages of the current query to fish.
        """
        return self._next_url is not None

    @GObject.Property
    def cookies(self):
        """
        The cookies this fisher currently sends to the webservice upon requests.
        """
        return self._cookies

    @cookies.setter
    @typechecked
    def cookies(self, cookies: RequestsCookieJar):
        """
        Set the cookies this fisher sends to the webservice upon requests.
        Can be used to make the fisher take over another session.
        """
        self._cookies = cookies

    @GObject.Property  # read-only
    def headers(self):
        """
        The HTTP headers this fisher currently sends to the webservice upon
        requests.
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

    @abc.abstractmethod
    def _find_next_url(self, page_soup: BeautifulSoup) -> str:
        """
        Finds the URL to the next results page using the *page_soup* of
        the current result page. Returns *None* if there is no such URL.
        To be overridden in subclasses.
        """

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
    def fish_next_page(self, page_soup: BeautifulSoup = None,
                       start_at: int = 0) -> Iterator[X]:
        """
        Fish all results from the page represented by *soup* and the
        result position on this page represented by *start_at*.

        If *soup* is None, fish from the result page that follows
        the previously fished page. If this previous page was the
        last page of the Google Scholar results a `StopIteration`
        exception is raised.

        To avoid overloading the google servers there should be a delay
        between fishing subsequent search pages.
        """
        if not self.has_next_page:
            raise StopIteration()

        with self._create_http_session() as session:
            if page_soup is None:
                page_soup = self._get_soup_from_url(session, self.next_url)

            yield from self._do_fish(session, page_soup, start_at)

            self._next_url = self._find_next_url(page_soup)
            self.notify('next-url')
            if self._next_url is None:
                self.notify('has-next-page')

    @abc.abstractmethod
    def _do_fish(self, session: requests.Session, page_soup: BeautifulSoup,
                 start_at: int) -> Iterator[X]:
        """
        Fish result objects from a result page of the webservice
        given by *soup*.

        :param session: The HTTP session to be used for making additional
            queries (if desired)
        :param page_soup: The `BeautifulSoup` representing the current
            result page
        :param start_at: The index of the result on the result page where
            fishing must begin
        :return: the result objects
        """
        pass

    @abc.abstractmethod
    def _resolve_fishing_error(self, e: FishingError) -> BeautifulSoup:
        """
        If an exception *e* is caught while fishing a page, this method is
        called before fishing is retried.
        Subclasses may decide to implement an error handling for specific
        error causes by implementing this method.
        They must then return a *BeautifulSoup* which represents the same page
        with the error resolved.
        A typical use case is solving a captcha that is blocking the page.

        The default implementation simply returns the unchanged failed page.
        """
        return e.failure_soup

    def _sleep_between_requests(self, mean_delay: float):
        delta = .7 * mean_delay
        time.sleep(mean_delay + random.uniform(-delta, +delta))

    @typechecked
    def fish_all(self, max_retries: int=3, mean_delay: float=0) -> Iterator[X]:
        """
        Blocking method to fish all results currently looked for by this fisher.
        If an error is caught, the *_resolve_fishing_error* method is called to
        tackle it. Afterwards, the fishing is retried at the same position.
        At most *max_retries* retries are attempted.

        *mean_delay* specifies a sleep time between the fishing on subsequent
        pages. This can be used to reduce the load on the webservice.
        The delay is randomized a bit to appear more natural.
        """
        soup = None
        retries = 0

        while True:
            try:
                yield from self.fish_next_page(soup)
            except FishingError as e:
                retries += 1

                if retries > max_retries:
                    raise e

                soup = self._resolve_fishing_error(e)
            else:
                soup = None
                retries = 0

                if not self.has_next_page:
                    break

                if mean_delay:
                    self._sleep_between_requests(mean_delay)
