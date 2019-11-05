import random
import time
from typing import TypeVar, Iterable, Iterator
from urllib.parse import urlparse

from typeguard import typechecked
from requests.cookies import RequestsCookieJar

import pubfisher.scrapers as gs
from bs4 import BeautifulSoup

import gi
gi.require_version('Gtk', '3.0')
gi.require_version('WebKit2', '4.0')
from gi.repository import Gtk, WebKit2, Soup, GLib, GObject


class CaptchaSolverWebView(WebKit2.WebView):
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
        :param captcha_html: The HTML of the captcha page that was displayed instead
        :param user_agent: The user agent used when the captcha occurred
        :param session_cookies: The Google Scholar session cookies used when the captcha occurred
        """
        super(CaptchaSolverWebView, self).__init__()

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


class UserAbandonedCaptchaException(Exception):
    pass


def _on_captcha_solved(web_view, unlocked_html, window):
    window.destroy()


def _on_window_destroy(*args):
    Gtk.main_quit()


def _solve_captcha(e: gs.ScrapeError):
    window = Gtk.Window()
    window.set_title("Solve Captcha")
    window.connect("destroy", _on_window_destroy)

    ls_cookies = [Soup.Cookie.new(bs_cookie.name,
                                  bs_cookie.value,
                                  bs_cookie.domain,
                                  bs_cookie.path,
                                  bs_cookie.expires)
                  for bs_cookie in e.cookies]

    web_view = CaptchaSolverWebView(e.continuation_url,
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


def _sleep_between_requests(average_delay: float):
    delta = .7 * average_delay
    time.sleep(average_delay + random.uniform(-delta, +delta))


X = TypeVar('X')


@typechecked
def scrape_interactive(scraper: gs.GScraper[X],
                       mean_delay: float = 3.0,
                       max_retries: int = 3) -> Iterator[X]:
    """
    Blocking method to scrape all publications targeted by *scraper*.
    If the scraper is stopped by a captcha, a GUI is displayed to solve it.
    Afterwards, the scraping continues.

    :param scraper: the scraper to use for retrieving results
    :param mean_delay: average delay until the next GS results page is retrieved
    :param max_retries: the tolerated number of subsequent failed requests
    """

    soup = None
    retries = 0

    while True:
        try:
            yield from scraper.scrape_next_page(soup)
        except gs.ScrapeError as e:
            retries += 1

            if retries > max_retries:
                raise e

            if isinstance(e.cause, gs.CaptchaRequiredError):
                unlocked_html, new_session_cookies = _solve_captcha(e)
                # use the just unlocked html, instead of reloading the same page
                soup = BeautifulSoup(unlocked_html, 'html.parser')
                scraper.cookies = new_session_cookies
            else:
                # retry with the same soup
                soup = e.failure_soup
        else:
            soup = None
            retries = 0

            if scraper.has_next_page:
                _sleep_between_requests(mean_delay)
            else:
                break
