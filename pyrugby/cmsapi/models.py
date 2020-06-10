from typing import List, Optional
import logging
from dataclasses import dataclass
from functools import cached_property

import requests

from .utils import get_api_url, cms_timestamp_to_datetime

log = logging.getLogger(__name__)


class Timeline():
    def __init__(self, match_id):
        self.match_id = match_id
        self.json = self._get_data()
        self.teams = {
            i: e['id'] for
            i, e in enumerate(self.json['match']['teams'])
        }

    def _get_data(self):
        url = get_api_url('match_timeline', {'match_id': self.match_id})
        r = requests.get(url)
        data = r.json()
        return data

    def _parse_timeline(self):
        match_events = []
        for e in self.json['timeline']:
            pos = e.get('position', {})
            tstamp = e.get('timestamp', {})
            event = MatchEvent(
                phase=e.get('phase'),
                match_time=e.get('time', {}).get('secs'),
                event=e.get('type'),
                label=e.get('typeLabel'),
                team_id=self.teams.get(e.get('teamIndex')),
                player_id=e.get('playerId'),
                points=e.get('points', 0),
                x_pos=pos.get('x'),
                y_pos=pos.get('y'),
                ex_pos=pos.get('ex'),
                ey_pos=pos.get('ey'),
                m_pos=pos.get('m'),
                info=','.join(e.get('info', [])),
                millis=tstamp.get('millis', None),
                gmt_offset=tstamp.get('gmtOffset', 0)
            )
            match_events.append(event)
        return match_events

    @cached_property
    def events(self):
        return self._parse_timeline()

    def infill_timestamps(self):
        for i, e in enumerate(self.events):
            if e.millis is None:
                log.debug(
                    "Event index: %d | Match time: %d", i, e.match_time
                )
                for e2 in self.events[:i][::-1]:
                    if e2.match_time and e2.millis:
                        e.millis = e2.millis + (e.match_time - e2.match_time)
                        e.gmt_offset = e2.gmt_offset
                        log.debug("New millis: %d", e.millis)
                        break
                else:
                    log.warn(
                        "No suitable time correction found for: %s", e
                    )

    def calculate_scores(self):
        idmap = {v: k for k, v in self.teams.items()}
        score = {0: 0, 1: 0}
        for e in self.events:
            if e.team_id:
                score[idmap[e.team_id]] += e.points
            e.score0 = score[0]
            e.score1 = score[1]


@dataclass
class MatchEvent():
    phase: str
    match_time: int
    event: str
    label: str
    team_id: int
    player_id: int
    points: int
    x_pos: int
    y_pos: int
    ex_pos: int
    ey_pos: int
    m_pos: int
    info: str
    millis: int
    gmt_offset: int
    score0: int = None
    score1: int = None

    @property
    def adjusted_time(self):
        if self.millis is None:
            return None
        return cms_timestamp_to_datetime(
            self.millis/1000, self.gmt_offset
        )


@dataclass
class Country():
    id: int
    name: str


@dataclass
class Venue():
    id: int
    city: str
    country: str
    names: List[str]


@dataclass
class Team():
    id: int
    sport: int
    team_type: int
    country_id: int
    name: str
    short_name: str


@dataclass
class Player():
    id: int
    initials: str
    first_name: str
    first_name_full: str
    last_name: str
    display_name: str
    pob: str
    dob: int
    country: str
    gender: str
    hof: Optional[int]
    first_match: Optional[int]
    last_match: Optional[int]
