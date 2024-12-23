from enum import Enum
from typing import Optional, Union, List
from pathlib import Path

from lxml import etree
from utils import get_metadata_profile, make_record_iri, get_id, NAMESPACES, NAMESPACES_19139, NAMESPACES_19115_1, send_query_to_db, str_tidy, replace_all
from hashlib import sha1

from rdflib import Graph, BNode, Literal, URIRef
from rdflib.namespace import RDF, SDO

from time import perf_counter

import pickle


class Profile(Enum):
    SEADATANET = "SeaDataNet"
    ISO19115 = "ISO19115"
    ISO19139 = "ISO19139"
    UNKNOWN = "Unknown"


def make_thesaurus_iri(name: str):
    return "http://example.com/thesaurus/" + str(sha1(name.encode()).hexdigest())


def get_kws_per_thes(kw_set: etree, prefix, prefix_2, namespaces) -> []:
    kws = []

    theme = kw_set.xpath(
        f"{prefix}:type/{prefix}:MD_KeywordTypeCode/@codeListValue",
        namespaces=namespaces,
    )

    md_keywords = kw_set.xpath(
        f"{prefix}:keyword",
        namespaces=namespaces,
    )
    for md_keyword in md_keywords:
        text_keywords = md_keyword.xpath(
            f"gco:CharacterString/text()",
            namespaces=namespaces,
        )

        anchor_keywords = md_keyword.xpath(
            f"gmx:Anchor",
            namespaces=namespaces)

        improved_anchor_keywords = []
        for ak in anchor_keywords:
            link = ak.xpath("./@xlink:href", namespaces=namespaces)


            if len(link) >= 1 and link[0] != "":
                # when the anchor is telling us that the kw is a 'theme' kw...
                if link[0] == "http://inspire.ec.europa.eu/theme/of":
                    # print({"value": str_tidy(ak.text), "theme": "theme"})
                    kws.append({"value": str_tidy(ak.text), "theme": "theme"})
                else:
                    improved_anchor_keywords.append(link[0])
            elif ak.text is not None:
                improved_anchor_keywords.append(str_tidy(ak.text))
            else:
                # discard empty result
                continue

        for x in text_keywords + improved_anchor_keywords:
            kw = x if x.startswith("http") else str_tidy(x)
            th = theme[0] if len(theme) > 0 else None
            kws.append({"value": kw, "theme": th})

    return kws


def match_thes_to_kb(thes_iri: str, thes_name: str) -> {}:
    # see if we have an aliasFor IRI for this thesaurus
    q = """
        PREFIX sa: <https://w3id.org/semanticanalyser/>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

        SELECT ?iri ?name
        WHERE {
          GRAPH sa:system-graph {
            ?iri sa:hasAlias <XXXX> ;
                skos:prefLabel ?name .
          }
        }    
        """.replace("XXXX", thes_iri)
    r = send_query_to_db(q)
    if len(r) > 0:
        return r[0]["iri"]["value"], r[0]["name"]["value"]

    if thes_name is not None:
        # if not, see if we can name match the thesaurus
        q = """
            PREFIX dcat: <http://www.w3.org/ns/dcat#>
            PREFIX sa: <https://w3id.org/semanticanalyser/>
            PREFIX schema: <https://schema.org/>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

            SELECT ?iri ?l
            WHERE {
              {
                SELECT DISTINCT ?iri ?l ?w ?p ?theme
                WHERE {
                  GRAPH sa:system-graph {
                    {     
                      BIND (10 AS ?w)
                      ?iri skos:prefLabel ?l .          

                      FILTER (?l = "XXXX")
                    }
                    UNION
                    { 
                      BIND (9 AS ?w)
                      ?iri skos:altLabel ?l .

                      FILTER (?l = "XXXX")
                    }
                    UNION
                    {     
                      BIND (8 AS ?w)
                      ?iri skos:prefLabel ?l .          

                      FILTER (CONTAINS(?l, "XXXX"))
                    }
                    UNION
                    { 
                      BIND (7 AS ?w)
                      ?iri skos:altLabel ?l .

                      FILTER (CONTAINS(?l, "XXXX"))
                    }

                    OPTIONAL {
                      ?iri sa:hasPreference [
                        schema:value ?p ;
                        dcat:theme ?theme ;
                      ] .        
                    }
                  }  
                }
                ORDER BY DESC(?w)
              }
              FILTER (?w > 8)  # exact prefLabel and altLabel matches only for now
          }
        """.replace("XXXX", thes_name)
        r = send_query_to_db(q)
        if len(r) > 0:
            return r[0]["iri"]["value"], r[0]["l"]["value"]

    return None, None


def match_thesaurus(thesaurus, namespaces, prefix_2):
    thesaurus_iris = thesaurus.xpath(f"@xlink:href", namespaces=namespaces)
    if len(thesaurus_iris) > 0:
        thesaurus_iri = thesaurus_iris[0]
    else:
        thesaurus_iris = thesaurus.xpath(
            f"{prefix_2}:CI_Citation/{prefix_2}:identifier/{prefix_2}:MD_Identifier/{prefix_2}:code/gco:CharacterString/text()",
            namespaces=namespaces,
        )
        if len(thesaurus_iris) > 0:
            thesaurus_iri = thesaurus_iris[0]
        else:
            thesaurus_iris = thesaurus.xpath(
                f"{prefix_2}:CI_Citation/{prefix_2}:identifier/{prefix_2}:MD_Identifier/{prefix_2}:code/gmx:Anchor/@xlink:href",
                namespaces=namespaces,
            )
            if len(thesaurus_iris) > 0:
                thesaurus_iri = thesaurus_iris[0]
            else:
                thesaurus_iri = None

    # try and use thesaurus cache
    # a, b = thes_cache_get(thesaurus_iri)
    # if a is not None and b is not None:
    #     return a, b

    original_thesaurus_iri = thesaurus_iri

    thesaurus_names = thesaurus.xpath(f"@xlink:title", namespaces=namespaces)
    if len(thesaurus_names) > 0:
        thesaurus_name = thesaurus_names[0]
    else:
        thesaurus_names = thesaurus.xpath(
            f"{prefix_2}:CI_Citation/{prefix_2}:title/gco:CharacterString/text()",
            namespaces=namespaces,
        )
        if len(thesaurus_names) > 0:
            thesaurus_name = thesaurus_names[0]
        else:
            thesaurus_name = None

    # if we have a thesaurus IRI, see if we have an aliasFor IRI for it
    if thesaurus_iri is not None:
        alias_iri, alias_name = match_thes_to_kb(thesaurus_iri, thesaurus_name)
        if alias_iri is not None:
            thesaurus_iri = alias_iri
            thesaurus_name = alias_name

    # no matches of any kind so return nothing
    if thesaurus_iri is None and thesaurus_name is None:
        return None, None

    if thesaurus_iri is None and thesaurus_name is not None:
        thesaurus_iri = make_thesaurus_iri(thesaurus_name)

    improved_name = thesaurus_name.strip() if thesaurus_name is not None else None

    THES_CACHE.add((original_thesaurus_iri, thesaurus_iri, improved_name))
    return thesaurus_iri, improved_name


def get_thes_and_kws(path_to_file_or_etree: Union[Path, etree], profile: Optional[Profile] = None, doc_iri: Optional[str] = None) -> {}:
    et = path_to_file_or_etree if not isinstance(path_to_file_or_etree, Path) else etree.parse(path_to_file_or_etree)

    if profile is None:
        profile = get_metadata_profile(et)

    if doc_iri is None:
        doc_iri = make_record_iri(get_id(et, profile))

    if profile in [Profile.ISO19139, Profile.SEADATANET]:
        prefix = "gmd"
        prefix_2 = "gmd"
        namespaces = {**NAMESPACES, **NAMESPACES_19139}
    elif profile == Profile.ISO19115:
        prefix = "mri"
        prefix_2 = "cit"
        namespaces = {**NAMESPACES, **NAMESPACES_19115_1}
    else:
        prefix = "gmd"
        prefix_2 = "gmd"
        namespaces = {**NAMESPACES, **NAMESPACES_19139}

    theses = {}

    keyword_sets = et.xpath(
        "//gmd:MD_Keywords",
        namespaces=namespaces,
    )
    for keyword_set in keyword_sets:
        kws = get_kws_per_thes(keyword_set, prefix, prefix_2, namespaces)

        thesauruses = keyword_set.xpath(
            f"{prefix}:thesaurusName",
            namespaces=namespaces,
        )

        if len(thesauruses) < 1:  # i.e. this keyword_set has no thesaurus
            if theses.get("empty") is None:
                theses["empty"] = {
                    "name": "",
                    "keywords": []
                }

            theses["empty"] = {
                "name": "",
                "keywords": theses["empty"]["keywords"] + kws
            }
        else:
            for thesaurus in thesauruses:
                thes_iri, thes_name = match_thesaurus(thesaurus, namespaces, prefix_2)

                theses[thes_iri] = {
                    "name": thes_name,
                    "keywords": kws
                }

    return doc_iri, theses


def match_kw_to_kb(kw_text: str, kw_iri: str = None, thes_iri: str = None) -> str:
    if kw_iri is None and kw_text is None:
        return None

    # try cache
    x = cache_get(kw_text, thes_iri)
    if x is not None:
        return x

    # try well-known IRIs
    if kw_iri is not None:
        if "https://www.ncei.noaa.gov/archive/accession/" in kw_iri:
            return kw_iri
        if "https://www.ncei.noaa.gov/archive/archive-management-system" in kw_iri:
            return kw_iri
        if kw_iri.startswith("http://vocab.nerc.ac.uk"):
            return kw_iri

    if kw_iri is not None and kw_text is None:
        return kw_iri

    # tidy the text
    if kw_text.startswith("What:"):
        kw_text = kw_text.replace("What: ", "")
        kw_text = kw_text.split(";")[0].strip()
    if "/" in kw_text:
        if kw_text.endswith("/"):
            kw_text = kw_text.split("/")[-2].strip()
        else:
            kw_text = kw_text.split("/")[-1].strip()
    if ">" in kw_text:
        kw_text = kw_text.split(">")[-1].strip()

    # try matching to an ID (notation)
    if "_" in kw_text:
        # this looks like an ID, so try to match it to a notation
        q = """
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            
            SELECT ?iri
            WHERE {
                ?iri
                    a skos:Concept ;
                    skos:prefLabel "XXX"@en ;
                .
            }
            """.replace("XXX", kw_text)

        r = send_query_to_db(q)

        if len(r) > 0:
            return r[0]["iri"]["value"]

    # we haven't nicely matched it to an ID, so remove the "_" to allow for better text matching
    kw_text = kw_text.replace("_", " ")


    # searching by IRI
    if thes_iri is not None:
        if kw_iri is not None:
            q = """
                PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

                SELECT ?iri ?pl ?weight
                WHERE {
                  GRAPH <XXX> {
                    BIND (<YYY> AS ?iri)
                    
                    ?iri 
                      a skos:Concept ; 
                        skos:prefLabel ?pl ;
                    .
                    
                    BIND (10 AS ?weight)
                  }
                }
                LIMIT 3
                """.replace("XXX", thes_iri).replace("YYY", kw_iri)
        else:
            q = """
                PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
                
                SELECT ?iri ?pl ?weight
                WHERE {
                  GRAPH <YYY> {
                    {
                      BIND (10 AS ?weight)
                      ?iri 
                        a skos:Concept ; 
                          skos:notation ?pl ;
                       .
                       FILTER (STR(?pl) = "ZZZ")
                    }
                    UNION    
                    {
                      BIND (9 AS ?weight)
                      ?iri 
                        a skos:Concept ; 
                          skos:prefLabel ?pl ;
                      .
                      FILTER (STR(?pl) = "ZZZ")
                    }
                    UNION
                    {
                      BIND (8 AS ?weight)
                      ?iri 
                        a skos:Concept ; 
                          skos:altLabel ?pl ;
                       .
                       FILTER (STR(?pl) = "ZZZ")
                    }    
                    UNION 
                    {
                     BIND (7 AS ?weight)
                     ?iri 
                       a skos:Concept ; 
                         skos:prefLabel ?pl ;
                      .
                      FILTER (REGEX (?pl, "ZZZ", "i"))
                    }
                    UNION 
                    {
                     BIND (6 AS ?weight)
                     ?iri 
                       a skos:Concept ; 
                         skos:altLabel ?pl ;
                      .
                      FILTER (REGEX (?pl, "ZZZ", "i"))
                    }      
                  }
                }
                ORDER BY DESC(?weight)
                LIMIT 3        
                """.replace("YYY", thes_iri).replace("ZZZ", kw_text)
    else:
        if kw_iri is not None:
            q = """
                PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

                SELECT ?iri ?pl ?weight
                WHERE {
                    BIND (<YYY> AS ?iri)

                    ?iri 
                      a skos:Concept ; 
                        skos:prefLabel ?pl ;
                    .

                    BIND (10 AS ?weight)
                }
                LIMIT 3
                """.replace("YYY", kw_iri)
        else:
            q = """
                PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
                PREFIX text:    <http://jena.apache.org/text#>
                
                SELECT *
                WHERE {
                    (?iri ?score ?pl ?g) text:query ("ZZZ")
                    
                    FILTER (?score > 8)
                }
                ORDER BY DESC(?score)
                LIMIT 3
                """.replace("ZZZ", kw_text.replace(":", " ").replace(",", ""))

    r = send_query_to_db(q)

    if len(r) > 0:
        return r[0]["iri"]["value"]


    # if the kw_text is really an IRI
    if kw_text.startswith("http"):
        q = """
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            
            ASK
            WHERE {
                <XXX> a skos:Concept .
            }            
            """.replace("XXX", kw_iri)

        if send_query_to_db(q):
            return kw_iri


    # full-text search using value
    if kw_text:
        q = """
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            PREFIX text:    <http://jena.apache.org/text#>
    
            SELECT *
            WHERE {
                (?iri ?score ?pl ?g) text:query ("ZZZ")
                
                FILTER (?score > 8)
            }
            ORDER BY DESC(?score)
            LIMIT 3
            """.replace("ZZZ", kw_text.replace(":", " ").replace(",", ""))

        r = send_query_to_db(q)

        if len(r) > 0:
            return r[0]["iri"]["value"]

    # got nuthin' so return original text
    return kw_text


def get_best_guess_kws(path_to_file_or_etree: Union[Path, etree]):
    et = path_to_file_or_etree if not isinstance(path_to_file_or_etree, Path) else etree.parse(path_to_file_or_etree)

    doc_iri, thesauri = get_thes_and_kws(et)

    for thesaurus, content in thesauri.items():
        thesaurus = None if thesaurus == "empty" else thesaurus

        improved_kws = []
        for kw in content["keywords"]:
            kw_iri = kw["value"] if kw["value"].startswith("http") else None
            val = match_kw_to_kb(kw["value"], kw_iri, thesaurus)

            theme = kw["theme"]
            improved_kws.append({
                "value": val,
                "theme": theme,
                "thesaurus": thesaurus,
                "original": kw["value"]
            })

        thesauri["empty" if thesaurus is None else thesaurus]["keywords"] = improved_kws

    return doc_iri, thesauri


def sample_records(n: int) -> List[Path]:
    import random

    pathlist = Path("/Users/nick/Work/bodc/fair-ease/kwextractor/records/records").glob("*.xml")

    rc = []
    for k, path in enumerate(pathlist):
        if k < n:
            rc.append(path)  # because path is object not string
        else:
            i = random.randint(0, k)
            if i < n:
                rc[i] = path

    return rc


def present_results(thesauri):
    kw_only = []
    for thesaurus, content in thesauri.items():
        for kw in content["keywords"]:
            kw_only.append((kw["theme"], kw["value"], kw.get("thesaurus"), kw["original"]))

    from operator import itemgetter
    # kw_only.sort(key=itemgetter(0, 1))

    from prettytable import PrettyTable
    table = PrettyTable()
    table.field_names = ["Theme", "Keyword", "Thesaurus", "Original"]
    for kw in kw_only:
        table.add_row([kw[0], kw[1], kw[2], kw[3]])
    print(table)


def cache_add(thesauri):
    for thesaurus, content in thesauri.items():
        for kw in content["keywords"]:
            KW_CACHE.add((kw["original"], kw["thesaurus"], kw["value"]))


def cache_get(value, thesaurus):
    for entry in KW_CACHE:
        if entry[0] == value and entry[1] == thesaurus:
            return entry[2]
    else:
        return None


def thes_cache_get(thes_iri):
    for entry in THES_CACHE:
        if entry[0] == thes_iri:
            return entry[1], entry[2]
    return None, None


def convert_results_to_graph(thesauri: {}, doc_iri: str) -> Graph:
    g = Graph()
    doc_iri = URIRef(doc_iri)
    g.add((doc_iri, RDF.type, SDO.CreativeWork))
    for thesaurus, content in thesauri.items():
        for kw in content["keywords"]:
            if kw["value"].startswith("http"):
                kw_iri = URIRef(kw["value"])
            else:
                kw_iri = BNode()

            g.add((kw_iri, RDF.type, SDO.DefinedTerm))

            if kw["original"] != kw["value"]:
                if kw["original"].startswith("http"):
                    g.add((kw_iri, SDO.replacee, URIRef(kw["original"])))
                else:
                    if isinstance(kw_iri, BNode):
                        g.add((kw_iri, SDO.value, Literal(kw["original"])))
                    else:
                        c = BNode()
                        g.add((kw_iri, SDO.citation, c))
                        g.add((c, SDO.value, Literal(kw["original"])))
                        g.add((c, SDO.isBasedOn, doc_iri))
            else:
                if not kw["original"].startswith("http"):
                    g.add((kw_iri, SDO.value, Literal(kw["original"])))

            if kw["thesaurus"] is not None:
                if kw["thesaurus"].startswith("http"):
                    g.add((kw_iri, SDO.inDefinedTermSet, URIRef(kw["thesaurus"])))
                else:
                    g.add((kw_iri, SDO.inDefinedTermSet, Literal(kw["thesaurus"])))

            g.add((doc_iri, SDO.keywords, kw_iri))
    g.add((doc_iri, RDF.type, SDO.CreativeWork))

    return g


def cache_prep(kw_cache_file, KW_CACHE):
    if len(KW_CACHE) == 0:
        if Path(kw_cache_file).is_file():
            KW_CACHE = pickle.load(open(kw_cache_file, "rb"))
        else:
            KW_CACHE = [
                ("earth science > paleoclimate > tree ring",
                 "https://gcmd.earthdata.nasa.gov/kms/concepts/concept_scheme/sciencekeywords ",
                 "https://gcmd.earthdata.nasa.gov/kms/concept/0e06e528-e796-4b7c-9878-dbcb061d878d"),
                ("What: total ring width; Material: null",
                 "https://www.ncei.noaa.gov/access/paleo-search/cvterms?termId=3639"),
                ("What: tree ring standardized growth index; Material: null",
                 "https://www.ncei.noaa.gov/access/paleo-search/cvterms?termId=682"),
                ("What: age; Material: null", "https://www.ncei.noaa.gov/access/paleo-search/cvterms?termId=241")
            ]
            pickle.dump(KW_CACHE, open(kw_cache_file, "wb"))

    print(f"KW_CACHE: {len(KW_CACHE)}")


def cache_store(kw_cache_file, KW_CACHE):
    pickle.dump(KW_CACHE, open(kw_cache_file, "wb"))


if __name__ == "__main__":
    THES_CACHE = set()
    KW_CACHE = set()
    kw_cache_file = "KW_CACHE.p"

    cache_prep(kw_cache_file, KW_CACHE)

    t1_start = perf_counter()

    # records =  sample_records(3)

    # records = [Path("/Users/nick/Work/bodc/sa-records/ifremer/0098a856-7401-46c2-9f7a-a2e5c0cf899c.xml")]
    #   * missing target application vocab online - https://sextant.ifremer.fr/geonetwork/srv/api/registries/vocabularies/local.target-application.myocean.target-application

    # records =  [Path("/Users/nick/Work/bodc/sa-records/ifremer/sdn-open:urn:SDN:CDI:LOCAL:696-486-486-ds07-4.xml")]
    # 100%

    # records =  [Path("/Users/nick/Work/bodc/sa-records/ifremer/f632d0d4-3373-43a4-a6be-d2109ebe0177.xml")]
    # 100%

    # records =  [Path("/Users/nick/Work/bodc/sa-records/ifremer/01f36842-927c-43a8-a531-f3c5096f3b34.xml")]
    # 21/37 - no thes for non-matches

    # records =  [Path("/Users/nick/Work/bodc/sa-records/ifremer/0c6e6b99-eaa6-41f7-b9f5-8a84d60b02b0.xml")]
    # 100s of Accession Records
    # GCMD Providers vocab missed <https://gcmd.earthdata.nasa.gov/kms/concept/086c68e5-1c94-4f2f-89d5-0453443ff249> - added
    # GCSM Science Keywords missing <https://gcmd.earthdata.nasa.gov/kms/concept/cd5a4729-ea4a-4ce1-8f5a-ec6a76d31055> - not yet added

    # records =  [Path("/Users/nick/Work/bodc/sa-records/ifremer/a54ac0ea-b4f9-48cb-ae55-f84c78848a28.xml")]
    # 100%

    # records =  [Path("/Users/nick/Work/bodc/sa-records/noaa-paleoclimatolog/0be5e200-0742-4744-8b31-337f7144d444.xml")]
    # 23s

    # records =  [Path("/Users/nick/Work/bodc/sa-records/noaa-paleoclimatolog/0be5e200-0742-4744-8b31-337f7144d444.2.xml")]
    # dedupe
    # 4s

    # record_sample = [Path("/home/nick/work/bodc/sa-records/paleoclimatolog_04/fff282ca-6097-4660-a416-73d3ba3d768f.xml")]
    # 2s

    #record_sample = [Path("/Users/nick/Work/bodc/sa-records/noaa-paleoclimatolog-dedupe/fff282ca-6097-4660-a416-73d3ba3d768f.xml")]
    # dedupe
    # 2s

    # records =  [Path("/Users/nick/Work/bodc/sa-records/noaa-paleoclimatolog-dedupe/93c28262-4a53-4383-966e-8f088e8a3723.xml")]

    # records =  sorted(Path("/Users/nick/Work/bodc/sa-records/noaa-paleoclimatolog-dedupe").glob("*.xml"))
    # records =  [Path(__file__).parent.parent.resolve().parent / "sa-records/cmems/0048F0DF85529BD09AA6FD32D8BD6DBB0F34AD89.xml"]

    folder = "capital"
    resulting_nt_file = Path(__file__).parent / f"{folder}-keywords.nt"
    records = sorted(Path(f"/home/nick/work/bodc/sa-records/{folder}").glob("*.xml"))
    start = 0
    count = 0
    for r in records[start:]:
        print(r)
        # get the KWs
        doc_iri, thesauri = get_best_guess_kws(r)
        # print the results to screen
        present_results(thesauri)
        # save the results to an RDF file
        with open(resulting_nt_file, "a") as f:
            f.write(convert_results_to_graph(thesauri, doc_iri).serialize(format="nt"))
            f.write("\n\n")
        # put results in the cache
        cache_add(thesauri)

        count += 1
        print(f"record no. {count}")
        print(f"cache len. {len(KW_CACHE)}")

    cache_store(kw_cache_file, KW_CACHE)

    t1_stop = perf_counter()
    print("Elapsed time :", t1_stop - t1_start)

    # process_all_records(5000, 1001)


