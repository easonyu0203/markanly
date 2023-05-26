from datetime import datetime, timedelta
from typing import Iterator, List, Tuple

import numpy as np
from sqlalchemy import and_
from tqdm import tqdm

from models import BehaviorData
from utils.db_utils import create_db_session


class DatasetGenerator:

    def __init__(self, start_date: datetime = datetime(2023, 1, 1), end_date: datetime = datetime(2023, 2, 1)):
        """
        we will generate dataset from start_date to end_date(exclusive)

        """
        self.start_date = start_date
        self.end_date = end_date
        self._db_session = create_db_session()
        self._chunk_size = 100_000

    def __del__(self):
        # close db session
        self._db_session.close()

    def generate_dataset(self) -> Tuple[np.ndarray, np.ndarray]:
        """Generate dataset.
            return dataset_size of features and labels,
            features shape: (dataset_size, feature_dim)
            labels shape: (dataset_size, 1)
        Returns:
            (np.ndarray, np.ndarray): X, Y
        """
        print(f'generate dataset...')
        X, Y = [], []
        for session in self._get_sessions():
            # filter unwanted sessions
            if not self._is_wanted_session(session):
                continue

            # sub-session
            for i in range(1, len(session)):
                sub_session = session[:i]

                # feature transformation
                x = self._transform(sub_session)
                y = self._label(sub_session, session)
                X.append(x)
                Y.append(y)
        print(f'generate dataset done.')

        X, Y = np.array(X), np.array(Y)
        return X, Y

    def _is_wanted_session(self, session: list[BehaviorData]) -> bool:
        """TODO: Check if this session is wanted.
        for given session, determine whether it is wanted or not.
        Args:
            session: list of BehaviorData within this session
        Returns:
            is_wanted_session: bool
        """
        # below is an example implementation

        # only one action we don't want
        if len(session) <= 1:
            return False

        # session duration < 1 min we don't want
        if session[-1].EventTime - session[0].EventTime <= timedelta(minutes=1):
            return False

        return True

    def _transform(self, sub_session: List[BehaviorData]) -> np.ndarray:
        """
        TODO: Transform sub-session to feature vector.
        the return value should be a 1-d numpy array.
        Args:
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

    def _label(self, sub_session: List[BehaviorData], session: List[BehaviorData]) -> float:
        """
        TODO: Label sub-session.
        the return value should be a float number which represents how many minute this session have lasted.
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

    def _get_sessions(self) -> Iterator[list[BehaviorData]]:
        """Get all sessions in the given time range."""

        # Execute the query and get all sessions
        query = self._db_session.query(BehaviorData). \
            filter(and_(BehaviorData.HitTime >= self.start_date,
                        BehaviorData.HitTime < self.end_date)). \
            order_by(BehaviorData.FullvisitorId, BehaviorData.HitTime, BehaviorData.EventTime)

        visitor_id, hit_time = None, None
        session = []
        session_cnt = 0
        for behaviorData in tqdm(query.yield_per(self._chunk_size)):
            # check is this behaviorData in the same session
            if (visitor_id is None and hit_time is None) or \
                    (visitor_id == behaviorData.FullvisitorId and hit_time == behaviorData.HitTime):
                # same session
                session.append(behaviorData)
                visitor_id, hit_time = behaviorData.FullvisitorId, behaviorData.HitTime
            else:
                # new session
                session_cnt += 1
                yield session
                visitor_id, hit_time = behaviorData.FullvisitorId, behaviorData.HitTime
                session = [behaviorData]


if __name__ == '__main__':
    # example of how to use
    dataset_generator = DatasetGenerator(start_date=datetime(2019, 1, 1), end_date=datetime(2019, 2, 1))
    X, Y = dataset_generator.generate_dataset()
    print(X.shape, Y.shape)
