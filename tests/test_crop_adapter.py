from agri_data_gen.core.data_access.adapters.crop_adapter import CropAdapter

def test_crop_adapter():
    adapter = CropAdapter("data/raw/Crop_recommendation.csv")
    adapter.load()

    all_ids = adapter.get_all_ids()
    print("Crop IDs:", all_ids)

    sample = adapter.sample(all_ids[0])
    print("Sample output:", sample)


test_crop_adapter()