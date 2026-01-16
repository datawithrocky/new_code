import json
from file_map import file_map


def standardization_process(mapping_payload=None, json_path="json"):
    if mapping_payload is None:
        with open(json_path, "r", encoding="utf-8") as f:
            mapping_payload = json.load(f)
            print(mapping_payload)

    return file_map(mapping_payload)


if __name__ == "__main__":
    result = standardization_process()
    print(result)
