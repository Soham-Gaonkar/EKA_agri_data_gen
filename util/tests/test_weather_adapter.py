from agri_data_gen.core.data_access.adapters.weather_adapter import WeatherAdapter

def test_weather():
    adapter = WeatherAdapter("data/raw/weather.csv")
    adapter.load()

    ids = adapter.get_all_ids()
    print("Weather IDs:", ids[:5])

    sample = adapter.sample(ids[0])
    print("Sample weather object:\n", sample)


test_weather()