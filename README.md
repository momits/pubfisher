# Pubfisher: Effectively explore scientific publications

Pubfisher is about querying scientific publications from web sources
such as Google Scholar.
These sources often do not offer a convenient, programmable API,
such that complex or comprehensive queries require lots of manual
steps, such as solving Captchas.

Pubfisher offers a simple data model and API that reduces the manual
effort to the minimum and makes complex queries simple to express.
In particular, Pubfisher does not fail when it sees a Captcha:
It shows the captcha to you, you solve the captcha, on goes the query.

Let's say you are interested in the first 200 citations of a paper
according to Google Scholar
Then this could be your query:
```python
from pubfisher.fishers.googlescholar import PublicationGSFisher
from itertools import islice


def my_query():
    fisher = PublicationGSFisher()
    
    fisher.look_for_key_words('Parachute use to prevent death '
                              'and major trauma related to '
                              'gravitational challenge: '
                              'systematic review of randomised '
                              'controlled trials')
    
    return islice(fisher.fish_all(), 200)
```

Using one and the same scraper, you can perform lots of queries.
Pubfisher takes care of reusing the session cookies across requests
such that your queries appear natural to the underlying web services.
