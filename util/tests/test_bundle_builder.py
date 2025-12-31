from agri_data_gen.core.knowledge.bundle_builder import BundleBuilder

def test_bundle_builder():
    builder = BundleBuilder(out_dir="data/bundles")
    builder.load_all()

    bundles = builder.build_all()

    print(f"Generated {len(bundles)} bundles.")
    print("Sample bundle:", bundles[0])

test_bundle_builder()