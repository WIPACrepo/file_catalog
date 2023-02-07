# test_urlargparse2.py
"""Unit tests for file_catalog/urlargparse2.py."""

import pytest
from tornado.escape import url_unescape

from file_catalog.urlargparse import encode as old_encode
from file_catalog.urlargparse import parse as old_parse
from file_catalog.urlargparse2 import encode as encode
from file_catalog.urlargparse2 import parse as parse


def test_00_always_succeed() -> None:
    """Succeed with flying colors."""
    assert True


def test_01_jquery_with_old_encode() -> None:
    """Test jQuery.param style encoding with old_encode."""
    # See: https://api.jquery.com/jQuery.param/
    OBJ = {"a": [2, 3, 4]}
    ANS = "a%5B%5D=2&a%5B%5D=3&a%5B%5D=4"
    assert "a[]=2&a[]=3&a[]=4" == url_unescape(ANS)
    assert ANS == old_encode(OBJ)  # type: ignore[no-untyped-call]


def test_02_jquery_with_old_encode() -> None:
    """Test jQuery.param style encoding with old_encode."""
    # See: https://api.jquery.com/jQuery.param/
    OBJ = {"a": {"b": 1, "c": 2}, "d": [3, 4, {"e": 5}]}
    ANS = "a%5Bb%5D=1&a%5Bc%5D=2&d%5B%5D=3&d%5B%5D=4&d%5B2%5D%5Be%5D=5"
    assert "a[b]=1&a[c]=2&d[]=3&d[]=4&d[2][e]=5" == url_unescape(ANS)
    assert ANS == old_encode(OBJ)  # type: ignore[no-untyped-call]


def test_03_jquery_with_old_parse_broken() -> None:
    """Test jQuery.param style parsing is broken with old_parse."""
    # See: https://api.jquery.com/jQuery.param/
    OBJ = "a%5B%5D=2&a%5B%5D=3&a%5B%5D=4"
    assert "a[]=2&a[]=3&a[]=4" == url_unescape(OBJ)
    # Should be: ANS = {"a": [2, 3, 4]}
    ANS = {'a': {'': 4}}
    assert ANS == old_parse(OBJ)


def test_04_jquery_with_old_parse_broken() -> None:
    """Test jQuery.param style parsing is broken with old_parse."""
    # See: https://api.jquery.com/jQuery.param/
    OBJ = "a%5Bb%5D=1&a%5Bc%5D=2&d%5B%5D=3&d%5B%5D=4&d%5B2%5D%5Be%5D=5"
    assert "a[b]=1&a[c]=2&d[]=3&d[]=4&d[2][e]=5" == url_unescape(OBJ)
    # Should be: ANS = {"a": {"b": 1, "c": 2}, "d": [3, 4, {"e": 5}]}
    ANS = {'a': {'b': 1, 'c': 2}, 'd': {2: {'e': 5}, '': 4}}
    assert ANS == old_parse(OBJ)


def test_05_jquery_with_encode() -> None:
    """Test jQuery.param style encoding with encode."""
    # See: https://api.jquery.com/jQuery.param/
    OBJ = {"a": [2, 3, 4]}
    ANS = "a%5B%5D=2&a%5B%5D=3&a%5B%5D=4"
    assert "a[]=2&a[]=3&a[]=4" == url_unescape(ANS)
    assert ANS == encode(OBJ)


def test_06_jquery_with_encode() -> None:
    """Test jQuery.param style encoding with encode."""
    # See: https://api.jquery.com/jQuery.param/
    OBJ = {"a": {"b": 1, "c": 2}, "d": [3, 4, {"e": 5}]}
    ANS = "a%5Bb%5D=1&a%5Bc%5D=2&d%5B%5D=3&d%5B%5D=4&d%5B2%5D%5Be%5D=5"
    assert "a[b]=1&a[c]=2&d[]=3&d[]=4&d[2][e]=5" == url_unescape(ANS)
    assert ANS == encode(OBJ)


def test_07_complicated_with_encode() -> None:
    """Test jQuery.param style encoding with encode."""
    # See: https://api.jquery.com/jQuery.param/
    OBJ = {
        "a": None,
        "b": 0,
        "c": -0.5,
        "d": 0.5,
        "e": "hello",
        "f": {
            "g": "1",
            "h": 1,
            "i": None,
        },
        "j": [
            "1",
            1,
            None,
        ],
        "k": {
            "l": ["1", "2", "3"],
            "m": [1.0, 2.0, 3.0],
            "n": [None, None, None],
        },
        "o": [
            {
                "p": "one",
                "q": "two",
                "r": "three",
            },
            {
                "s": -1.0,
                "t": -2.0,
                "u": -3.0,
            },
            {
                "v": None,
                "w": None,
                "x": None,
            },
        ],
        "y": {
            "ya": [
                {"yaaa": 1, "yaab": 2, "yaac": 3},
                {"yaba": 1, "yabb": 2, "yabc": 3},
                {"yaca": 1, "yacb": 2, "yacc": 3},
            ],
        },
        "z": [
            {
                "zaa": ["zaaa", "zaab", "zaac"],
                "zab": ["zaba", "zabb", "zabc"],
                "zac": ["zaca", "zacb", "zacc"],
            },
        ],
    }
    ANS = "a=&b=0&c=-0.5&d=0.5&e=hello&f%5Bg%5D=1&f%5Bh%5D=1&f%5Bi%5D=&j%5B%5D=1&j%5B%5D=1&j%5B%5D=&k%5Bl%5D%5B%5D=1&k%5Bl%5D%5B%5D=2&k%5Bl%5D%5B%5D=3&k%5Bm%5D%5B%5D=1&k%5Bm%5D%5B%5D=2&k%5Bm%5D%5B%5D=3&k%5Bn%5D%5B%5D=&k%5Bn%5D%5B%5D=&k%5Bn%5D%5B%5D=&o%5B0%5D%5Bp%5D=one&o%5B0%5D%5Bq%5D=two&o%5B0%5D%5Br%5D=three&o%5B1%5D%5Bs%5D=-1&o%5B1%5D%5Bt%5D=-2&o%5B1%5D%5Bu%5D=-3&o%5B2%5D%5Bv%5D=&o%5B2%5D%5Bw%5D=&o%5B2%5D%5Bx%5D=&y%5Bya%5D%5B0%5D%5Byaaa%5D=1&y%5Bya%5D%5B0%5D%5Byaab%5D=2&y%5Bya%5D%5B0%5D%5Byaac%5D=3&y%5Bya%5D%5B1%5D%5Byaba%5D=1&y%5Bya%5D%5B1%5D%5Byabb%5D=2&y%5Bya%5D%5B1%5D%5Byabc%5D=3&y%5Bya%5D%5B2%5D%5Byaca%5D=1&y%5Bya%5D%5B2%5D%5Byacb%5D=2&y%5Bya%5D%5B2%5D%5Byacc%5D=3&z%5B0%5D%5Bzaa%5D%5B%5D=zaaa&z%5B0%5D%5Bzaa%5D%5B%5D=zaab&z%5B0%5D%5Bzaa%5D%5B%5D=zaac&z%5B0%5D%5Bzab%5D%5B%5D=zaba&z%5B0%5D%5Bzab%5D%5B%5D=zabb&z%5B0%5D%5Bzab%5D%5B%5D=zabc&z%5B0%5D%5Bzac%5D%5B%5D=zaca&z%5B0%5D%5Bzac%5D%5B%5D=zacb&z%5B0%5D%5Bzac%5D%5B%5D=zacc"
    assert "a=&b=0&c=-0.5&d=0.5&e=hello&f[g]=1&f[h]=1&f[i]=&j[]=1&j[]=1&j[]=&k[l][]=1&k[l][]=2&k[l][]=3&k[m][]=1&k[m][]=2&k[m][]=3&k[n][]=&k[n][]=&k[n][]=&o[0][p]=one&o[0][q]=two&o[0][r]=three&o[1][s]=-1&o[1][t]=-2&o[1][u]=-3&o[2][v]=&o[2][w]=&o[2][x]=&y[ya][0][yaaa]=1&y[ya][0][yaab]=2&y[ya][0][yaac]=3&y[ya][1][yaba]=1&y[ya][1][yabb]=2&y[ya][1][yabc]=3&y[ya][2][yaca]=1&y[ya][2][yacb]=2&y[ya][2][yacc]=3&z[0][zaa][]=zaaa&z[0][zaa][]=zaab&z[0][zaa][]=zaac&z[0][zab][]=zaba&z[0][zab][]=zabb&z[0][zab][]=zabc&z[0][zac][]=zaca&z[0][zac][]=zacb&z[0][zac][]=zacc" == url_unescape(ANS)
    assert ANS == encode(OBJ)


def test_08_pass_bad_data_to_encode() -> None:
    """Ensure that encode raises a TypeError when misused."""
    with pytest.raises(TypeError):
        encode(None)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        encode("xyz")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        encode([])  # type: ignore[arg-type]


def test_09_dicts_within_dicts_with_encode() -> None:
    """Test jQuery.param style encoding with encode."""
    # See: https://api.jquery.com/jQuery.param/
    OBJ = {"a": {"b": {"c": {"d": 1}}}}
    ANS = "a%5Bb%5D%5Bc%5D%5Bd%5D=1"
    assert "a[b][c][d]=1" == url_unescape(ANS)
    assert ANS == encode(OBJ)


def test_10_lists_within_lists_with_encode() -> None:
    """Test jQuery.param style encoding with encode."""
    # See: https://api.jquery.com/jQuery.param/
    OBJ = {"a": [[1, 2, 3], [4, [5, 6]], [7, [8, [9, [10, 11, 12, 13]]]]]}
    ANS = "a%5B0%5D%5B%5D=1&a%5B0%5D%5B%5D=2&a%5B0%5D%5B%5D=3&a%5B1%5D%5B%5D=4&a%5B1%5D%5B1%5D%5B%5D=5&a%5B1%5D%5B1%5D%5B%5D=6&a%5B2%5D%5B%5D=7&a%5B2%5D%5B1%5D%5B%5D=8&a%5B2%5D%5B1%5D%5B1%5D%5B%5D=9&a%5B2%5D%5B1%5D%5B1%5D%5B1%5D%5B%5D=10&a%5B2%5D%5B1%5D%5B1%5D%5B1%5D%5B%5D=11&a%5B2%5D%5B1%5D%5B1%5D%5B1%5D%5B%5D=12&a%5B2%5D%5B1%5D%5B1%5D%5B1%5D%5B%5D=13"
    assert "a[0][]=1&a[0][]=2&a[0][]=3&a[1][]=4&a[1][1][]=5&a[1][1][]=6&a[2][]=7&a[2][1][]=8&a[2][1][1][]=9&a[2][1][1][1][]=10&a[2][1][1][1][]=11&a[2][1][1][1][]=12&a[2][1][1][1][]=13" == url_unescape(ANS)
    assert ANS == encode(OBJ)


def test_11_jquery_with_parse() -> None:
    """Test jQuery.param style parsing is broken with old_parse."""
    # See: https://api.jquery.com/jQuery.param/
    OBJ = "a%5B%5D=2&a%5B%5D=3&a%5B%5D=4"
    assert "a[]=2&a[]=3&a[]=4" == url_unescape(OBJ)
    ANS = {"a": [2, 3, 4]}
    assert ANS == parse(OBJ)


def test_12_jquery_with_parse() -> None:
    """Test jQuery.param style parsing is broken with old_parse."""
    # See: https://api.jquery.com/jQuery.param/
    OBJ = "a%5Bb%5D=1&a%5Bc%5D=2&d%5B%5D=3&d%5B%5D=4&d%5B2%5D%5Be%5D=5"
    assert "a[b]=1&a[c]=2&d[]=3&d[]=4&d[2][e]=5" == url_unescape(OBJ)
    ANS = {"a": {"b": 1, "c": 2}, "d": [3, 4, {"e": 5}]}
    assert ANS == parse(OBJ)


def test_13_dicts_within_dicts_with_parse() -> None:
    """Test jQuery.param style encoding with encode."""
    # See: https://api.jquery.com/jQuery.param/
    OBJ = "a%5Bb%5D%5Bc%5D%5Bd%5D=1"
    assert "a[b][c][d]=1" == url_unescape(OBJ)
    ANS = {"a": {"b": {"c": {"d": 1}}}}
    assert ANS == parse(OBJ)


def test_14_lists_within_lists_with_parse() -> None:
    """Test jQuery.param style encoding with encode."""
    # See: https://api.jquery.com/jQuery.param/
    OBJ = "a%5B0%5D%5B%5D=1&a%5B0%5D%5B%5D=2&a%5B0%5D%5B%5D=3&a%5B1%5D%5B%5D=4&a%5B1%5D%5B1%5D%5B%5D=5&a%5B1%5D%5B1%5D%5B%5D=6&a%5B2%5D%5B%5D=7&a%5B2%5D%5B1%5D%5B%5D=8&a%5B2%5D%5B1%5D%5B1%5D%5B%5D=9&a%5B2%5D%5B1%5D%5B1%5D%5B1%5D%5B%5D=10&a%5B2%5D%5B1%5D%5B1%5D%5B1%5D%5B%5D=11&a%5B2%5D%5B1%5D%5B1%5D%5B1%5D%5B%5D=12&a%5B2%5D%5B1%5D%5B1%5D%5B1%5D%5B%5D=13"
    assert "a[0][]=1&a[0][]=2&a[0][]=3&a[1][]=4&a[1][1][]=5&a[1][1][]=6&a[2][]=7&a[2][1][]=8&a[2][1][1][]=9&a[2][1][1][1][]=10&a[2][1][1][1][]=11&a[2][1][1][1][]=12&a[2][1][1][1][]=13" == url_unescape(OBJ)
    ANS = {"a": [[1, 2, 3], [4, [5, 6]], [7, [8, [9, [10, 11, 12, 13]]]]]}
    assert ANS == parse(OBJ)


def test_15_complicated_with_parse() -> None:
    """Test jQuery.param style encoding with encode."""
    # See: https://api.jquery.com/jQuery.param/
    OBJ = "a=&b=0&c=-0.5&d=0.5&e=hello&f%5Bg%5D=1&f%5Bh%5D=1&f%5Bi%5D=&j%5B%5D=1&j%5B%5D=1&j%5B%5D=&k%5Bl%5D%5B%5D=1&k%5Bl%5D%5B%5D=2&k%5Bl%5D%5B%5D=3&k%5Bm%5D%5B%5D=1&k%5Bm%5D%5B%5D=2&k%5Bm%5D%5B%5D=3&k%5Bn%5D%5B%5D=&k%5Bn%5D%5B%5D=&k%5Bn%5D%5B%5D=&o%5B0%5D%5Bp%5D=one&o%5B0%5D%5Bq%5D=two&o%5B0%5D%5Br%5D=three&o%5B1%5D%5Bs%5D=-1&o%5B1%5D%5Bt%5D=-2&o%5B1%5D%5Bu%5D=-3&o%5B2%5D%5Bv%5D=&o%5B2%5D%5Bw%5D=&o%5B2%5D%5Bx%5D=&y%5Bya%5D%5B0%5D%5Byaaa%5D=1&y%5Bya%5D%5B0%5D%5Byaab%5D=2&y%5Bya%5D%5B0%5D%5Byaac%5D=3&y%5Bya%5D%5B1%5D%5Byaba%5D=1&y%5Bya%5D%5B1%5D%5Byabb%5D=2&y%5Bya%5D%5B1%5D%5Byabc%5D=3&y%5Bya%5D%5B2%5D%5Byaca%5D=1&y%5Bya%5D%5B2%5D%5Byacb%5D=2&y%5Bya%5D%5B2%5D%5Byacc%5D=3&z%5B0%5D%5Bzaa%5D%5B%5D=zaaa&z%5B0%5D%5Bzaa%5D%5B%5D=zaab&z%5B0%5D%5Bzaa%5D%5B%5D=zaac&z%5B0%5D%5Bzab%5D%5B%5D=zaba&z%5B0%5D%5Bzab%5D%5B%5D=zabb&z%5B0%5D%5Bzab%5D%5B%5D=zabc&z%5B0%5D%5Bzac%5D%5B%5D=zaca&z%5B0%5D%5Bzac%5D%5B%5D=zacb&z%5B0%5D%5Bzac%5D%5B%5D=zacc"
    assert "a=&b=0&c=-0.5&d=0.5&e=hello&f[g]=1&f[h]=1&f[i]=&j[]=1&j[]=1&j[]=&k[l][]=1&k[l][]=2&k[l][]=3&k[m][]=1&k[m][]=2&k[m][]=3&k[n][]=&k[n][]=&k[n][]=&o[0][p]=one&o[0][q]=two&o[0][r]=three&o[1][s]=-1&o[1][t]=-2&o[1][u]=-3&o[2][v]=&o[2][w]=&o[2][x]=&y[ya][0][yaaa]=1&y[ya][0][yaab]=2&y[ya][0][yaac]=3&y[ya][1][yaba]=1&y[ya][1][yabb]=2&y[ya][1][yabc]=3&y[ya][2][yaca]=1&y[ya][2][yacb]=2&y[ya][2][yacc]=3&z[0][zaa][]=zaaa&z[0][zaa][]=zaab&z[0][zaa][]=zaac&z[0][zab][]=zaba&z[0][zab][]=zabb&z[0][zab][]=zabc&z[0][zac][]=zaca&z[0][zac][]=zacb&z[0][zac][]=zacc" == url_unescape(OBJ)
    ANS = {
        "a": None,
        "b": 0,
        "c": -0.5,
        "d": 0.5,
        "e": "hello",
        "f": {
            "g": 1,
            "h": 1,
            "i": None,
        },
        "j": [
            1,
            1,
            None,
        ],
        "k": {
            "l": [1, 2, 3],
            "m": [1, 2, 3],
            "n": [None, None, None],
        },
        "o": [
            {
                "p": "one",
                "q": "two",
                "r": "three",
            },
            {
                "s": -1,
                "t": -2,
                "u": -3,
            },
            {
                "v": None,
                "w": None,
                "x": None,
            },
        ],
        "y": {
            "ya": [
                {"yaaa": 1, "yaab": 2, "yaac": 3},
                {"yaba": 1, "yabb": 2, "yabc": 3},
                {"yaca": 1, "yacb": 2, "yacc": 3},
            ],
        },
        "z": [
            {
                "zaa": ["zaaa", "zaab", "zaac"],
                "zab": ["zaba", "zabb", "zabc"],
                "zac": ["zaca", "zacb", "zacc"],
            },
        ],
    }
    assert ANS == parse(OBJ)


def test_16_evil_input_list_reassignment() -> None:
    """Test jQuery.param style encoding with encode."""
    # See: https://api.jquery.com/jQuery.param/
    OBJ = "a%5B%5D=1&a%5B%5D=2&a%5B%5D=3&a[0]=4"
    # we push 1, 2, 3 to the list a, then *reassign* a[0] = 4, overwriting the original 1
    assert "a[]=1&a[]=2&a[]=3&a[0]=4" == url_unescape(OBJ)
    ANS = {"a": [4, 2, 3]}
    assert ANS == parse(OBJ)
