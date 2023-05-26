from typing import Protocol, List

import numpy as np

from models import BehaviorData


class IFeatureTransformer(Protocol):

    def process(self, sub_session: List[BehaviorData], session: List[BehaviorData]) -> np.ndarray:
        """
        transform sub_session to feature vector.
        sub_session is a part of session, session is a whole session.
        Args:
            session: List of BehaviorData within this session
            sub_session: list of BehaviorData within this sub-session
        Returns:
            feature_vector: np.ndarray with shape (feature_dim,)
        """
        ...


class FeatureTransformer(IFeatureTransformer):

    def process(self, sub_session: List[BehaviorData], session: List[BehaviorData]) -> np.ndarray:
        """
        TODO: Transform sub-session to feature vector.
        sub_session is a part of session, session is a whole session.
        Args:
            session: List of BehaviorData within this session
            sub_session: list of BehaviorData within this sub-session
        Returns:
            feature_vector: np.ndarray with shape (feature_dim,)
        """
        # here is an example implementation

        # feature 1: session duration
        session_duration = sub_session[-1].EventTime - sub_session[0].EventTime

        # feature 2: number of actions
        num_actions = len(sub_session)

        # feature 3: number of unique pages
        num_unique_pages = len(set([s.PageType for s in sub_session if s.PageType is not None]))

        return np.array([session_duration, num_actions, num_unique_pages])
