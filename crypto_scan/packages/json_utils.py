import json
def load_data_from_json(filename):
    with open(filename, 'r') as f:
        prices = json.load(f)
    return prices


def save_data_to_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f)