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


def process_uid_data(uid: str, publication_objects: List[dict]) -> List[Tuple[int, str]]:
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
            creator_uid = "-".join(header.split('-')[2:])
            if "translation" in header and "en" in header and value:
                matching_publication = [obj for obj in publication_objects if obj['text_uid'] ==
                                        uid and obj['creator_uid'] == creator_uid and obj['translation_lang_iso'] == "en"]
                if len(matching_publication) == 1:
                    result.append(
                        ((matching_publication[0], row[0]), value.strip()))
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
            processed_data = process_uid_data(uid, publication_objects)
            if processed_data:
                output_data = []

                for (publication, verse_id), content in processed_data:
                    output_data.append({
                        "verse_id": verse_id,
                        "verse_text": content,
                        "book": publication.get('root_title', ''),
                        "translation": ', '.join(publication.get('creator_name', '')) if isinstance(publication.get('creator_name', ''), list) else publication.get('creator_name', '') or publication.get('translation_title', '')
                    })

                publication_title = publication.get('root_title', '')
                output_file_path = f"{publication_title}.json"
                with open(output_file_path, 'w', encoding='utf-8') as f:
                    json.dump(output_data, f, ensure_ascii=False, indent=4)

                print(f"Data for UID {uid} written to {output_file_path}")
        except Exception as e:
            print(f"Error processing UID {uid}: {str(e)}")
            processed_data = []
        print("---")
    print("Total number of lines processed across all UIDs: ",
          total_processed_lines)
