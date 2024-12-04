from lxml import etree

from pathlib import Path
from utils import NAMESPACES, NAMESPACES_19139

namespaces = {**NAMESPACES, **NAMESPACES_19139}

paleo = 0
for f in Path("/Users/nick/Work/bodc/sa-records/no-dashes").glob("*.xml"):
    print(f)
    et = etree.parse(f)
    title = et.xpath(
        f"//gmd:title/gco:CharacterString/text()",
        namespaces=namespaces,
    )
    if len(title) > 0:
        if title[0].startswith("NOAA/WDS Paleoclimatolog"):
            paleo += 1
            print(paleo)