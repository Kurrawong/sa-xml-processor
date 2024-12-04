# this script moves XML records from a given directory - FROM-DIR -
# selected by a simple text match in it - PHRASE -
# and places them into another directory - TO_DIR

from pathlib import Path


FROM_DIR = Path("/Users/nick/Work/bodc/sa-records/noaa-paleoclimatolog")
TO_DIR = Path(*[x.replace("noaa-paleoclimatolog", "noaa-paleoclimatolog-dedupe") for x in list(FROM_DIR.parts)])

i = 0
for record in FROM_DIR.glob("*.xml"):
    txt = record.read_text()
    if "PHRASE" in txt:
        record.rename(TO_DIR)
        i += 1
        print(i)

    # if i > 10: break
