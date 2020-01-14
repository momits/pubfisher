from gi.repository import GObject
from typing import Optional

from typeguard import typechecked


class Document(GObject.GObject):

    title = GObject.Property(type=str)
    authors = GObject.Property(type=str)
    abstract = GObject.Property(type=str)

    @typechecked
    def __init__(self,
                 title: str,
                 authors: str,
                 abstract: str,
                 citation_count: Optional[int]=None,
                 gs_doc_id: Optional[str] = None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.title = title
        self.authors = authors
        self.abstract = abstract
        self._citation_count = citation_count
        self._gs_doc_id = gs_doc_id

    @GObject.Property(type=int, minimum=0)  # read-only
    def citation_count(self):
        return self._citation_count

    @GObject.Property(type=str)
    def gs_doc_id(self):
        return self._gs_doc_id

    def __eq__(self, other):
        if not isinstance(other, Document):
            return False
        if self.gs_doc_id and other.gs_doc_id:
            return self.gs_doc_id == other.gs_doc_id
        else:
            return self.title == other.title

    def __str__(self):
        return '{}: {}'.format(self.authors, self.title)


class Publication(GObject.GObject):

    url = GObject.Property(type=str)
    eprint = GObject.Property(type=str, default=None)
    bibtex = GObject.Property(type=str)

    @typechecked
    def __init__(self,
                 document: Document,
                 year: Optional[int],
                 url: str,
                 eprint: Optional[str]=None,
                 gs_pub_id: Optional[str] = None,
                 bibtex: Optional[str] = None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self._document = document
        self._year = year
        self.url = url
        self.eprint = eprint
        self._gs_pub_id = gs_pub_id
        self.bibtex = bibtex

    @GObject.Property  # read-only
    def document(self):
        return self._document

    @GObject.Property
    def year(self):
        return self._year

    @year.setter
    @typechecked
    def year(self, year: Optional[int]):
        self._year = year

    @GObject.Property(type=str)  # read-only
    def gs_pub_id(self):
        return self._gs_pub_id

    @GObject.Property(type=str)  # read-only
    def abbreviation(self):
        fst_author = self.document.authors.split(', ')[0].split(' ')[1]
        fst_author = fst_author[:10] + (fst_author[10:] and '...')
        year = ' {}'.format(self.year) if self.year else ''
        abbrev = '[{}{}]'.format(fst_author, year)
        return abbrev

    def __str__(self):
        return '{} {}'.format(self.abbreviation, self.document.title)
