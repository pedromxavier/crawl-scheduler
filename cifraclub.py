import re
from string import ascii_uppercase as CAPITALS
from scheduler import BaseCrawler, BaseScheduler

RE_FLAGS = re.UNICODE | re.MULTILINE

BASE_URL = r'https://www.cifraclub.com.br'

class ArtistScheduler(BaseScheduler):

    @classmethod
    def schedule(cls, data: list, *args: tuple, **kwargs: dict) -> list:
        return [(f'{BASE_URL}/letra/{key}/lista.html',) for key in CAPITALS]

class ArtistCrawler(BaseCrawler):

    RE_ARTISTS = re.compile(r'<li><a href="/([a-zA-Z0-9\-]+?)/">(.+?)</a></li>', RE_FLAGS)

    @classmethod
    def crawl(cls, html_text: str, *args: tuple, **kwargs: dict) -> list:
        return [m.group(1) for m in cls.RE_ARTISTS.finditer(html_text)][1:-5]

class SongScheduler(BaseScheduler):

    @classmethod
    def schedule(cls, data: list) -> list:
        return [(f'{BASE_URL}/{artist}/', artist) for artist in data]

class SongCrawler(BaseCrawler):

    RE_SONG = lambda artist: re.compile(f'<a  href="/{re.escape(artist)}/([a-zA-Z0-9\\-]+?)/" class="art_music-link" title=".+?">.*?</a>', RE_FLAGS)

    @classmethod
    def crawl(cls, html_text: str, artist: str, *args: tuple, **kwargs: dict):
        return [f"{artist}/{m.group(1)}" for m in cls.RE_SONG(artist).finditer(html_text)]

class DataScheduler(BaseScheduler):

    @classmethod
    def schedule(cls, data: list) -> list:
        return [(f'{BASE_URL}/{song_url}',) for song_url in data]

class DataCrawler(BaseCrawler):

    RE_CHORDS = re.compile(r'<b\>(.+?)\</b>', RE_FLAGS)
    RE_CHORDS_HTML = re.compile(r'<pre>(.+?)</pre>', RE_FLAGS)
    RE_TONE = re.compile(r'<a class="js-modal-trigger" href="\#" title="alterar o tom da cifra">([A-G])</a>', RE_FLAGS)
    RE_BREADCRUMB_HTML = re.compile(r'<div id="breadcrumb" class="g-1">(.+?)</div>', RE_FLAGS)
    RE_BREADCRUMB = re.compile(r'<span .+?itemprop="title">(.+?)</span>', RE_FLAGS)

    @classmethod
    def crawl(cls, html_text: str, *args: tuple, **kwargs: dict):
        ## Chords
        chords_match = cls.RE_CHORDS_HTML.search(html_text)

        if chords_match is None:
            return None
        else:
            chords_text = chords_match.group(1)
    
        chords = [m.group(1) for m in cls.RE_CHORDS.finditer(chords_text)]

        if not chords:
            return None

        ## Tone
        tone_match = cls.RE_TONE.search(html_text)

        if tone_match is None:
            return None
        else:
            tone = tone_match.group(1)

        ## Gender
        breadcrumb_match = cls.RE_BREADCRUMB_HTML.search(html_text)

        if breadcrumb_match is None:
            return None
        else:
            breadcrumb_text = breadcrumb_match.group(1)

            breadcrumb_info = [m.group(1) for m in cls.RE_BREADCRUMB.finditer(breadcrumb_text)]

            return [[
                breadcrumb_info[0],
                breadcrumb_info[1],
                breadcrumb_info[2],
                tone,
                " ".join(chords)
            ]]

if __name__ == '__main__':
    artist_sch = ArtistScheduler(fname='artist')
    artist_crl = ArtistCrawler(fname='artist')

    song_sch = SongScheduler(fname='song')
    song_crl = SongCrawler(fname='song')

    data_sch = DataScheduler(fname='data')
    data_crl = DataCrawler(fname='data')

    artist_sch >> artist_crl >> song_sch >> song_crl >> data_sch >> data_crl

    data_crl.save()