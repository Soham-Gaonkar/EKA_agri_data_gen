# src/agri_data_gen/core/data_access/adapters/base_adapter.py

from abc import ABC, abstractmethod
from typing import Dict, Any, List


class BaseAdapter(ABC):
    """
    Abstract base interface for all dataset adapters.
    Each adapter wraps one dataset and exposes:
    - load(): preparing the internal dataset structure
    - sample(entry_id): return structured data for one taxonomy entry
    - get_all_ids(): return all entry IDs supported by this adapter
    """

    @abstractmethod
    def load(self):
        """Load/prepare the dataset into memory."""
        raise NotImplementedError

    @abstractmethod
    def sample(self, entry_id: str) -> Dict[str, Any]:
        """Return a structured dict for the given taxonomy entry."""
        raise NotImplementedError

    @abstractmethod
    def get_all_ids(self) -> List[str]:
        """Return all IDs this adapter can serve."""
        raise NotImplementedError
