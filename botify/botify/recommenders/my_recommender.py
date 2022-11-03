from .toppop import TopPop
from .recommender import Recommender


class MyRecommender(Recommender):
    """
    Recommend personalized tracks for each user cached in Redis.
    Fall back to the random recommender if no recommendations found for the user.
    """

    def __init__(self, history_redis, recommendations_redis, catalog):
        self.recommendations_redis = recommendations_redis
        self.history_redis = history_redis
        self.fallback = TopPop(catalog.top_tracks[:100])
        self.catalog = catalog

    def recommend_next(self, user: int, prev_track: int, prev_track_time: float) -> int:
        recommendations_bytes = self.recommendations_redis.get(user)
        if recommendations_bytes is None:
            return self.fallback.recommend_next(user, prev_track, prev_track_time)

        history_bytes = self.history_redis.get(user)
        if history_bytes is None:
            return self.fallback.recommend_next(user, prev_track, prev_track_time)

        rec_history = set(list(self.catalog.from_bytes(history_bytes)))
        recommendations = list(self.catalog.from_bytes(recommendations_bytes))

        for recommendation in recommendations:
            if recommendation not in rec_history:
                return recommendation

        return self.fallback.recommend_next(user, prev_track, prev_track_time)
