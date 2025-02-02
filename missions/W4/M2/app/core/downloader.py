import os

import requests
from app.core.constants import (
    VehicleType,
    get_trip_data_path,
    get_trip_data_url,
)


def download_trip_data(
    vehicle_type: VehicleType, year: int, month: int
) -> bool:
    save_path = get_trip_data_path(vehicle_type, year, month)

    if os.path.exists(save_path):
        return True

    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    url = get_trip_data_url(vehicle_type, year, month)

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(save_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        return True

    except requests.RequestException as e:
        print(f"다운로드 실패: {e}")

        if os.path.exists(save_path):
            os.remove(save_path)

        return False
