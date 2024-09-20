import json
import pathlib
from typing import List, Tuple
from get_data import get_data


def load_publication():
    file_path = pathlib.Path(
        "/Users/jfuentes/Projects/B2Soul/bilara-data/_publication-v2.json")
    with open(file_path, "r") as file:
        return json.load(file)


def get_publication_objects():
    data = load_publication()
    publication_objects = []
    for item in data:
        if 'text_uid' in item and item.get('translation_lang_iso') == 'en':
            publication_objects.append(item)
    return publication_objects


def process_uid_data(uid: str) -> List[Tuple[int, str]]:
    repo_dir = pathlib.Path("/Users/jfuentes/Projects/B2Soul/bilara-data")
    include_filter = {frozenset(filter.split(
        '+')) if '+' in filter else filter for filter in "root,translation+en".split(',')}

    # Reset the generator
    data_generator = get_data(repo_dir, {uid}, include_filter)

    headers = next(data_generator)
    content = list(data_generator)

    result = []
    for row in content:
        for header, value in zip(headers, row):
            if "translation" in header and "en" in header and value:
                result.append(((row[0], header), value.strip()))
                break

    return result


if __name__ == "__main__":
    publication_objects = get_publication_objects()
    print(f"Total number of publication objects: {len(publication_objects)}")
    print("All publication objects with text UIDs:")
    for obj in publication_objects:
        print(f"{obj['text_uid']}")
    print("---")
    unique_uids = set(obj['text_uid'] for obj in publication_objects)
    total_processed_lines = 0
    for uid in unique_uids:
        print(f"Processing UID: {uid}")
        try:
            processed_data = process_uid_data(uid)
            if processed_data:
                num_lines = len(processed_data)
                total_processed_lines += num_lines
                print(f"Number of lines processed: {num_lines}")
                if num_lines > 0:
                    print(f"First line: {processed_data[0]}")
                if num_lines > 1:
                    print(f"Second line: {processed_data[1]}")
                if num_lines > 2:
                    print(f"Third line: {processed_data[2]}")
                if num_lines > 3:
                    print(f"Fourth line: {processed_data[3]}")
                if num_lines > 4:
                    print(f"Fifth line: {processed_data[4]}")
                if num_lines > 5:
                    print(f"Last line: {processed_data[-1]}")
            else:
                print("No data processed for this UID.")
        except Exception as e:
            print(f"Error processing UID {uid}: {str(e)}")
            processed_data = []
        print("---")
    print("Total number of lines processed across all UIDs: ",
          total_processed_lines)
