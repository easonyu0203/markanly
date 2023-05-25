from datetime import datetime, timedelta
from typing import Iterator, List, Tuple

import numpy as np
from sqlalchemy import and_

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

        X, Y = np.array(X), np.array(Y)
        return X, Y

    def _is_wanted_session(self, session: list[BehaviorData]) -> bool:
        """TODO: Check if this session is wanted."""
        # below is an example implementation

        # only one action we don't want
        if len(session) <= 1:
            return False

        # session duration < 1 min we don't want
        if session[-1].EventTime - session[0].EventTime <= timedelta(minutes=1):
            return False

        return True

    def _transform(self, sub_session: List[BehaviorData]) -> np.ndarray:
        pass

    def _label(self, sub_session: List[BehaviorData], session: List[BehaviorData]) -> float:
        pass

    def _get_sessions(self) -> Iterator[list[BehaviorData]]:
        """Get all sessions in the given time range."""
        query = self._db_session.query(BehaviorData). \
            filter(and_(BehaviorData.HitTime >= self.start_date,
                        BehaviorData.HitTime < self.end_date)). \
            group_by(BehaviorData.FullvisitorId, BehaviorData.HitTime)

        visitor_id, hit_time = None, None
        session = []
        for behaviorData in query:
            # check is this behaviorData in the same session
            if (visitor_id is None and hit_time is None) or \
                    (visitor_id == behaviorData.FullvisitorId and hit_time == behaviorData.HitTime):
                # same session
                session.append(behaviorData)
                visitor_id, hit_time = behaviorData.FullvisitorId, behaviorData.HitTime
            else:
                # new session
                yield session
                visitor_id, hit_time = behaviorData.FullvisitorId, behaviorData.HitTime
                session = [behaviorData]


if __name__ == '__main__':
    ...
    # dataset_generator = DatasetGenerator(start_date=datetime(2019, 1, 1), end_date=datetime(2019, 1, 2))
    # for session in dataset_generator._get_sessions():
    #     assert len(session) > 0
    #     assert all([s.FullvisitorId == session[0].FullvisitorId for s in session])
