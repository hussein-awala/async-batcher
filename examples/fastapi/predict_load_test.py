from __future__ import annotations

import random

from locust import HttpUser, between, task


class FastAPIUser(HttpUser):
    wait_time = between(2, 10)

    @task
    def predict(self):
        response = self.client.post(
            "/predict/",
            data=str([random.random() for _ in range(10)]),
            timeout=(1, 1),
        )
        assert response.status_code == 200
        assert isinstance(response.json(), float)
