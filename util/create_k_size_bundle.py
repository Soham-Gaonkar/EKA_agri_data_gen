import os
import random
import logging


def create_k_size_bundle(k: int):
    full_dataset_path = "data/bundles/bundles_hi.jsonl"
    test_dataset_path = f"data/bundles/test_{k}_samples.jsonl"

    with open(full_dataset_path, 'r', encoding='utf-8') as f:
        all_lines = f.readlines()
            
        # Safety check: Ensure we don't try to sample more than we have
        sample_size = min(k, len(all_lines))
        
        # Randomly select k lines (bundles)
        sampled_lines = random.sample(all_lines, sample_size)
        
        # Write these k lines to a new test file
        with open(test_dataset_path, 'w', encoding='utf-8') as f:
            f.writelines(sampled_lines)
            
        logger.info(f"Created test file with {sample_size} entries: {test_dataset_path}")


if __name__ == "__main__":
    create_k_size_bundle(300000)