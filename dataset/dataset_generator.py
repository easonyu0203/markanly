from datetime import datetime, timedelta
from typing import Iterator, List, Tuple

import numpy as np
from sqlalchemy import and_
from tqdm import tqdm

from dataset.Labeler import ILabeler
from dataset.feature_transformer import IFeatureTransformer
from dataset.session_filterer import ISessionFilterer
from models import BehaviorData
from utils.db_utils import create_db_session


class DatasetGenerator:

    def __init__(self,
                 session_filterer: ISessionFilterer,
                 feature_transformer: IFeatureTransformer,
                 labeler: ILabeler,
                 start_date: datetime = datetime(2023, 1, 1),
                 end_date: datetime = datetime(2023, 2, 1)):
        """
        we will generate dataset from start_date to end_date(exclusive)

        """
        self.start_date = start_date
        self.end_date = end_date
        self._db_session = create_db_session()
        self._chunk_size = 100_000

        # dependency injection
        self._session_filterer = session_filterer
        self._feature_transformer = feature_transformer
        self._labeler = labeler

    def __del__(self):
        # close db session
        self._db_session.close()

    def generate(self) -> Tuple[np.ndarray, np.ndarray]:
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
            # sub-session
            for i in range(1, len(session)):
                sub_session = session[:i]

                # filter unwanted sessions
                if not self._session_filterer.process(sub_session, session):
                    continue

                # feature transformation
                x = self._feature_transformer.process(sub_session, session)
                # labeling
                y = self._labeler.process(sub_session, session)
                X.append(x)
                Y.append(y)
        print(f'generate dataset done.')

        X, Y = np.array(X), np.array(Y)
        return X, Y

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
    X, Y = dataset_generator.generate()
    print(X.shape, Y.shape)
