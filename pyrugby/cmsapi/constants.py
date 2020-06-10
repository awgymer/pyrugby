EVENT_TYPES = {
    'MS': "Match Status",
    'U': "Pre-match",
    'L': "Live Match",
    'C': "Completed",
    'LD': "Match Delayed",
    'LHT': "Half Time",
    'L1': "First Half",
    'L2': "Second Half",
    'L3': "Extra Time First Half",
    'LB': "Extra Time Half Time",
    'L4': "Extra Time Second Half",
    'L6': "Extra Time",
    'LSD': "Sudden Death",
    'LFT': "Full Time",
    'LK': "Kicking Competition",
    'T5': "Try",
    'T4': "Try",
    'C2': "Conversion",
    'D3': "Drop Goal",
    'P3': "Penalty",
    'PT5': "Penalty Try",
    'Miss Con': "Missed Conversion",
    'Miss Pen': "Missed Penalty",
    'Miss DG': "Missed Drop Goal",
    'Sub On': "Sub On",
    'Sub Off': "Sub Off",
    'Yellow': "Yellow Card",
    'Red': "Red Card"
}

SPORTS_INT: {
  1: 'mru',
  2: 'wru',
  3: 'mrs',
  4: 'wrs',
  5: 'jmu',
  6: 'jwu',
  7: 'mjs',
  8: 'wjs'
}

SPORTS_FULL: {
    "Mens Rugby Union": 1,
    "Womens Rugby Union": 2,
    "Mens Rugby Sevens": 3,
    "Womens Rugby Sevens": 4,
    "Junior Mens Union": 5,
    "Junior Womens Union": 6,
    "Mens Junior Sevens": 7,
    "Womens Junior Sevens": 8
}

CMSAPI_SCHEME = "https"

CMSAPI_ROOT = "cmsapi.pulselive.com/"

CMSAPI_PATHS = {
    'match_search': "/rugby/match",
    'match': "/rugby/match/{match_id}",
    'match_stats': "/rugby/match/{match_id}/stats",
    'match_summary': "/rugby/match/{match_id}/summary",
    'match_timeline': "/rugby/match/{match_id}/timeline",
}
