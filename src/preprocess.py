import os
from collections import defaultdict

from tqdm import tqdm
import json

DATA_FILENAME = "data/dblp.v12.json"
VALID_FILENAME = "data/valid_ids.json"
INVALID_FILENAME = "data/invalid_ids.json"

cwd = os.path.dirname(__file__)
data_path = os.path.join(cwd, os.pardir, DATA_FILENAME)
valid_id_path = os.path.join(cwd, os.pardir, VALID_FILENAME)
invalid_id_path = os.path.join(cwd, os.pardir, INVALID_FILENAME)

def phase_1():
    invalid_ids = set()
    valid_ids = defaultdict(list)
    invalid_types = {"main": 0, "author": 0, "venue": 0, "citation": 0}

    main_keys = ["id", "authors", "title", "year", "n_citation", "references", "fos", "venue"]
    author_keys = ["name", "id"]
    venue_keys = ["raw"]


    with open(data_path, "r") as f:
        for i, line in enumerate(tqdm(f, total=4894084)):
            try:
                if line.startswith(","):
                    line = line.lstrip(",")
                
                item = eval(line)            
                id = item["id"]
                year = item["year"] if 2010 <= item["year"] <= 2019 else "others"

                isValid = True
                # Lack main keys or empty values for main keys
                if any(item.get(key, None) is None for key in main_keys):
                    isValid = False
                    invalid_types["main"] += 1
                
                # Lack author keys or empty values for author keys
                if isValid:
                    for author in item["authors"]:
                        if any(author.get(key, None) is None for key in author_keys):
                            isValid = False
                            invalid_types["author"] += 1
                            break

                # Lack venue keys or empty values for venue keys
                if isValid:
                    if any(item["venue"].get(key, None) is None for key in venue_keys):
                        isValid = False
                        invalid_types["venue"] += 1
                
                # n_citation < 5
                if isValid:
                    if item["n_citation"] < 5:
                        isValid = False
                        invalid_types["citation"] += 1
                    
                if isValid:
                    valid_ids[year].append(id)
                else:
                    invalid_ids.add(id)


            except SyntaxError:
                print("Line: {}: invalid syntax".format(i))
            
            except Exception as err:
                print("Line {}: {}".format(i, err))
        
        print(invalid_types)

        with open(valid_id_path, "w") as valid_f:
            json.dump(valid_ids, valid_f)
        
        with open(invalid_id_path, "w") as invalid_f:
            invalid_ids = list(invalid_ids)
            json.dump(invalid_ids, invalid_f)
    

def phase_2():
    valid_f = open(valid_id_path, "r")
    invalid_f = open(invalid_id_path, "r")

    valid_ids = json.load(valid_f)
    invalid_ids = json.load(invalid_f)
    invalid_ids = set(invalid_ids)
    
    with open(data_path, "r") as f:
        # for target_year in range(2010, 2020):
        #     print("========== Processing Year: {} ==========\n".format(target_year))
            # output = list()

        
        output = defaultdict(list)
        for i, line in enumerate(tqdm(f, total=4894084)):
            try:
                if line.startswith(","):
                    line = line.lstrip(",")
                
                item = eval(line)            
                id = item["id"]
                year = item["year"]

                # if year != target_year or id in invalid_ids: continue
                if year > 2019 or year < 2010 or id in invalid_ids: continue

                # Collect only necessary data for authors
                authors = list()
                for author in item["authors"]:
                    authors.append(
                        {
                            "name": author["name"],
                            "id": author["id"]
                        }
                    )

                # Clean-up references
                ref = [x for x in item["references"] if x not in invalid_ids]

                new_item = {
                    "id": id,
                    "authors": authors,
                    "title": item["title"],
                    "year": year,
                    "references": ref,
                    "fos": item["fos"],
                    "venue": item["venue"]["raw"]
                }
                # print(new_item)
                # output.append(new_item)
                output[year].append(new_item)

            except SyntaxError:
                print("Line: {}: invalid syntax".format(i))
            
            except Exception as err:
                print("Line {}: {}".format(i, err))
            
            
            # output_name = "data/dblp_{}.json".format(target_year)
            # output_path = os.path.join(cwd, os.pardir, output_name)
            # with open(output_path, "w") as output_f:
            #     json.dump(output, output_f)

        for year in range(2010, 2020):
            output_name = "data/dblp_{}.json".format(year)
            output_path = os.path.join(cwd, os.pardir, output_name)
            with open(output_path, "w") as output_f:
                json.dump(output[year], output_f)

    valid_f.close()
    invalid_f.close()


if __name__ == "__main__":
    # phase_1()
    phase_2()