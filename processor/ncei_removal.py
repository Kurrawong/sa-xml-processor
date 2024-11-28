from pathlib import Path


RECORDS_DIR = Path("/Users/nick/Work/bodc/sa-records/no-dashes")
NCEI_ACCESSION_DIR = Path("/Users/nick/Work/bodc/sa-records/ncei-accession")

i = 0
for record in RECORDS_DIR.glob("*.xml"):
    txt = record.read_text()
    if "NCEI Accession" in txt:
        new_file_path_parts = list(record.parts)[:-2]
        new_file_path_parts.append("ncei-accession")
        new_file_path_parts.append(record.name)
        new_file_path = Path(*new_file_path_parts)
        # print(record)
        # print("goes to")
        # print(new_file_path)
        record.rename(new_file_path)
        i += 1
        print(i)

    # if i > 10: break
