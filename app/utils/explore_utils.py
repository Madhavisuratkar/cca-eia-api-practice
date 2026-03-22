import os
import h5py
from app.utils.constants import PRICING_DATABASE

import os

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
h5_file_path = os.path.join(ROOT_DIR, 'database', PRICING_DATABASE)

with h5py.File(h5_file_path, 'r') as hdf:
    providers = list(hdf.keys())
    regions_map = {provider: list(hdf[provider].keys()) for provider in providers}
    instances_map = {
        (provider, region): set(hdf[f"{provider}/{region}"]['Instance'][:].astype(str))
        for provider, regions in regions_map.items()
        for region in regions
    }


def check_hdf5_regions(hdf, cloud_provider):
    if cloud_provider not in hdf:
        return False, [cloud_provider]
    return True, []


def check_hdf5_instance(hdf, cloud_provider, region):
    if cloud_provider not in hdf:
        return False, [cloud_provider]

    provider_group = hdf[cloud_provider]
    if region not in provider_group:
        return False, [region]

    return True, []


def get_all_instances_for_provider(provider):
    all_instances = set()
    for region in regions_map.get(provider, []):
        all_instances.update(instances_map.get((provider, region), set()))

    instance_type_data = sorted([str(item) for item in all_instances])
    return instance_type_data
