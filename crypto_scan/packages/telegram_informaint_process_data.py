import json



def load_data_from_json(filename):
    with open(filename, 'r') as f:
        prices = json.load(f)
    return prices


def save_data_to_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f)

def detect_changes(old_data,new_data):
    return new_data

def process_data(data):
    result = []
    for item in data:
        print(item)
        analythics_data = item["analythics"]
        result.append(analythics_data)
    return json.dumps(result, indent=4)


def main():
    old_data = load_data_from_json("../data/received_data_old.json")
    data = load_data_from_json("../data/received_data.json")
    changes = detect_changes(old_data,data)
    result = process_data(changes)
    print(result)
if __name__ == '__main__':
    main()