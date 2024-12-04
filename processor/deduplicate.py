# this script deduplicates multiple, identical peer keywords

from lxml import etree

from pathlib import Path
from utils import NAMESPACES, NAMESPACES_19139

if __name__ == "__main__":
    for f in Path("/Users/nick/Work/bodc/sa-records/noaa-paleoclimatolog").glob("*.xml"):
        print(f)
        et = etree.parse(f)
        keywords = et.xpath(
            f"//gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword",
            namespaces={**NAMESPACES, **NAMESPACES_19139},
        )
        kw_cache = []
        for keyword in keywords:
            this_kw_text = keyword.getchildren()[0].text
            if this_kw_text in kw_cache:
                keyword.getparent().remove(keyword)
            kw_cache.append(this_kw_text)

        f: Path
        new_f = Path(*[x.replace("noaa-paleoclimatolog", "noaa-paleoclimatolog-dedupe") for x in list(f.parts)])
        et.write(new_f, pretty_print=True)
