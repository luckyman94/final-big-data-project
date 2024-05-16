import sys
from unittest import mock

sys.path.append('/')
from src.scraping.allocine_scraper import scrap_allocine

def mock_response(url, headers):
    mock_resp = mock.Mock()
    mock_resp.status_code = 200
    mock_resp.content = """
    <div class="card entity-card entity-card-list cf">
        <h2><a>Movie Title</a></h2>
        <div class="meta-body-item meta-body-info">1h 30min</div>
        <span class="date">2022-01-01</span>
        <div class="meta-body-item meta-body-direction"><span class="dark-grey-link">Director Name</span></div>
        <div class="meta-body-item meta-body-actor"><a>Actor 1</a>, <a>Actor 2</a></div>
        <div class="rating-item"><span class="stareval-note">4.0</span></div>
        <div class="rating-item"><span class="stareval-note">3.5</span></div>
        <div class="content-txt">This is a synopsis.</div>
    </div>
    """
    return mock_resp

def test_scrap_allocine(mocker):
    mocker.patch('requests.get', side_effect=mock_response)

    genre_name = 'Comedy'
    genre_code = 13005
    num_pages = 1

    df = scrap_allocine(genre_name, genre_code, num_pages)

    assert df.shape[0] == 1
    assert df.iloc[0]['Title'] == 'Movie Title'
    assert df.iloc[0]['Duration'] == '1h 30min'
    assert df.iloc[0]['Genre'] == 'Comedy'
    assert df.iloc[0]['Release Date'] == '2022-01-01'
    assert df.iloc[0]['Director'] == 'Director Name'
    assert df.iloc[0]['Actors'] == 'Actor 1, Actor 2'
    assert df.iloc[0]['Press Rating'] == '4.0'
    assert df.iloc[0]['Spectator Rating'] == '3.5'
    assert df.iloc[0]['Synopsis'] == 'This is a synopsis.'
