from datetime import datetime
from typing import Iterator

from sqlalchemy import and_

from models import BehaviorData
from utils.db_utils import create_db_session


class DatasetGenerator:

    def __init__(self, start_date: datetime = datetime(2023, 1, 1), end_date: datetime = datetime(2023, 2, 1)):
        """

        Args:
            start_date: session star
            end_date:
        """
        self.start_date = start_date
        self.end_date = end_date
        self._db_session = create_db_session()
        self._chunk_size = 100_000

    def __del__(self):
        # close db session
        self._db_session.close()

    def _get_sessions(self) -> Iterator[list[BehaviorData]]:
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
    dataset_generator = DatasetGenerator(start_date=datetime(2019, 1, 1), end_date=datetime(2019, 1, 2))
    for session in dataset_generator._get_sessions():
        assert len(session) > 0
        assert all([s.FullvisitorId == session[0].FullvisitorId for s in session])

