import sys
import os
import pytest

# Ensure src/tunetracker is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
from tunetracker import producer


def test_random_user_id():
    msg = producer.generate_random_message()
    uid = msg["user_id"]
    assert isinstance(uid, str)
    assert uid.startswith("u")
    assert len(uid) == 5  # "u" + 4 digits


def test_random_track_id():
    msg = producer.generate_random_message()
    tid = msg["track_id"]
    assert isinstance(tid, str)
    assert tid.startswith("t")
    assert len(tid) == 7  # "t" + 6 digits


def test_random_genre():
    msg = producer.generate_random_message()
    genre = msg["genre"]
    assert genre in producer.GENRES


def test_random_message():
    msg = producer.generate_random_message()
    assert set(msg.keys()) == {"user_id", "track_id", "genre", "timestamp"}
    assert msg["genre"] in producer.GENRES
    assert isinstance(msg["user_id"], str)
    assert isinstance(msg["track_id"], str)
    assert isinstance(msg["timestamp"], str)
