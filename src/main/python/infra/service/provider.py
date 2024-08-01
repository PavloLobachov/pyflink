from typing import Any

from core.external_service import ExternalService, O

import covid_daily


class DataProvider(ExternalService[dict[str, Any]]):
    def get(self) -> O:
        return covid_daily.overview(as_json=True)[0]
