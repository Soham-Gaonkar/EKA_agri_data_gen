import typer
from agri_data_gen.core.data_access.taxonomy_manager import TaxonomyManager
from agri_data_gen.core.generators.generator import GenerationEngine
from agri_data_gen.core.knowledge.bundle_builder import BundleBuilder

app = typer.Typer()



@app.command()
def load_taxonomies(taxonomy_dir: str = "sample_data/taxonomies"):
    """
    Loads taxonomy schemas into MongoDB and prints a summary.
    """

    print("Loading taxonomies...")
    manager = TaxonomyManager()
    manager.load_from_files_and_store(taxonomy_dir)

    taxonomies = manager.get_active_taxonomies()
    print(f"\nLoaded {len(taxonomies)} active taxonomies:\n")

    for t in taxonomies:
        print(f"Group: {t['group']}")
        print(f"  Attributes: {len(t['attributes'])}")
        print(f"  Entries: {len(t['entries'])}")
        print("-" * 40)

@app.command()
def reset_taxonomies():
    """
    Completely clears the taxonomy collection in MongoDB.
    Use before reloading taxonomies after schema changes.
    """
    manager = TaxonomyManager()
    deleted = manager.reset_taxonomy_collection()
    print(f"Deleted {deleted} taxonomy entries.")


# @app.command()
# def generate_data(
#     bundle_dir: str = "data/bundles",
#     out_file: str = "data/generated/data.jsonl",
#     limit: int = None
# ):
#     """
#     Generate Hindi agronomic reasoning data from bundles.
#     """

#     engine = GenerationEngine(bundle_dir=bundle_dir, out_file=out_file)
#     engine.generate_all(limit=limit)


@app.command()
def pipeline_run(
    bundle_dir: str = "data/bundles",
    out_file: str = "data/generated/data.jsonl",
    limit: int = None
):
    """
    Run the full end-to-end pipeline:
    taxonomies → bundles → generation
    """

    print("Starting end-to-end pipeline...")

    # 1. Build bundles
    print("Building bundles...")
    bundle_builder = BundleBuilder(out_dir=bundle_dir)
    bundle_builder.load_all()
    bundles = bundle_builder.build_all()

    print(f"Generated {len(bundles)} bundles.")
    print("Sample bundle:", bundles[0])


    # 2. Generate data from bundles
    print("Generating reasoning data...")
    engine = GenerationEngine(bundle_dir=bundle_dir, out_file=out_file)
    engine.generate_all(limit=limit)

    print("Pipeline completed successfully.")


def main():

    app()

if __name__ == "__main__":
    main()
