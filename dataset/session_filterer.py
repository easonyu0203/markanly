from typing import Protocol, List

from models import BehaviorData


class ISessionFilterer(Protocol):
    def process(self, sub_session: list[BehaviorData], session: list[BehaviorData]) -> bool:
        """
        for given session, determine whether it is wanted or not.
        sub_session is a part of session, session is a whole session.
        Args:
            sub_session: list of BehaviorData within this sub-session
            session: list of BehaviorData within this session
        Returns:
            is_wanted_session: bool
        """
        ...


class SessionFilterer(ISessionFilterer):

    def __init__(self):
        """TODO: you can add some field here if you need."""
        ...

    def process(self, sub_session: List[BehaviorData], session: list[BehaviorData]) -> bool:
        """
        TODO: Check if this session is wanted.
        for given session, determine whether it is wanted or not.
        sub_session is a part of session, session is a whole session.
        Args:
            sub_session: list of BehaviorData within this sub-session
            session: list of BehaviorData within this session
        Returns:
            is_wanted_session: bool
        """
        # below is an example implementation

        # only one action we don't want
        if len(session) <= 1:
            return False

        return True
