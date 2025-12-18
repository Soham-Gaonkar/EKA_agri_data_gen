from typing import Dict, Any

class ValidityChecker:
    """
    Centralized rule engine for marking combinations as valid or invalid.
    Extend with soil, irrigation, topography rules later.
    """

    def __init__(self):
        pass

    def check_crop_weather_fit(self, crop: Dict[str, Any], weather: Dict[str, Any]) -> bool:
        """
        Validity rule:
        weather.mean_temp ∈ crop.temperature_tolerance
        AND
        weather.mean_precip ∈ crop.rainfall_range_mm
        """

        try:
            t_min, t_max = crop["temperature_tolerance"]
            r_min, r_max = crop["rainfall_range_mm"]

            t = weather["temperature_celsius"]["mean"]
            r = weather["precipitation_mm"]["mean"]

            temp_ok = (t_min <= t <= t_max)
            rain_ok = (r_min <= r <= r_max)

            return temp_ok and rain_ok
        except Exception:
            # Conservative: invalid if missing data
            return False
