from pathlib import Path


RECORDS_DIR = Path("/Users/nick/Work/bodc/sa-records/dashes")
NCEI_ACCESSION_DIR = Path("/Users/nick/Work/bodc/sa-records/ifremer")

i = 0
for record in RECORDS_DIR.glob("*.xml"):
    txt = record.read_text()
    if "ifremer" in txt:
        new_file_path_parts = list(record.parts)[:-2]
        new_file_path_parts.append("ifremer")
        new_file_path_parts.append(record.name)
        new_file_path = Path(*new_file_path_parts)
        # print(record)
        # print("goes to")
        # print(new_file_path)
        record.rename(new_file_path)
        i += 1
        print(i)

    # if i > 10: break
