from typing import Protocol, List

from models import BehaviorData


class ILabeler(Protocol):

    def process(self, sub_session: List[BehaviorData], session: List[BehaviorData]) -> float:
        """
        the return value should be a float number which represents of the label of this sub-session
        sub_session is a part of session, session is a whole session.
        Args:
            sub_session: list of BehaviorData within this sub-session
            session: list of BehaviorData within this session
        Returns:
            label: float number which represents of the label of this sub-session
        """
        ...


class Labeler(ILabeler):

    def process(self, sub_session: List[BehaviorData], session: List[BehaviorData]) -> float:
        """
        TODO: Label sub-session.
        the return value should be a float number which represents of the label of this sub-session
        sub_session is a part of session, session is a whole session.
        Args:
            sub_session: list of BehaviorData within this sub-session
            session: list of BehaviorData within this session
        Returns:
            label: float number which represents of the label of this sub-session
        """

        # here is an example implementation

        # label: session duration
        session_duration = session[-1].EventTime - sub_session[-1].EventTime

        return session_duration.total_seconds() / 60
