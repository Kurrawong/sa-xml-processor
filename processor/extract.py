from enum import Enum
from typing import Optional, Union, List
from pathlib import Path

from lxml import etree
from utils import get_metadata_profile, make_record_iri, get_id, NAMESPACES, NAMESPACES_19139, NAMESPACES_19115_1, send_query_to_db, str_tidy, replace_all
from hashlib import sha1

from rdflib import Graph, BNode, Literal, URIRef
from rdflib.namespace import RDF, SDO

import time


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

            # when the anchor is telling us that the kw is a 'theme' kw...
            if len(link) >= 1:
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
        thesauruses = keyword_set.xpath(
            f"{prefix}:thesaurusName",
            namespaces=namespaces,
        )

        kws = get_kws_per_thes(keyword_set, prefix, prefix_2, namespaces)

        for thesaurus in thesauruses:
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

            if thesaurus_name is not None and thesaurus_iri is None:
                thesaurus_iri = make_thesaurus_iri(thesaurus_name)

            if thesaurus_iri is not None:
                theses[thesaurus_iri] = {
                    "name": thesaurus_name.strip() if thesaurus_name is not None else None,
                    "keywords": kws
                }

        if len(thesauruses) < 1:  # i.e. this keyword_set has no thesaurus
            if theses.get("") is None:
                theses[""] = {
                    "name": "",
                    "keywords": []
                }

            theses[""]["keywords"] = theses[""]["keywords"] + kws

    return doc_iri, theses


def match_thes_to_kb(thes_iri: str, thes_name: str) -> {}:
    # see if we have an aliasFor IRI for this thesaurus
    q = """
        PREFIX sa: <https://w3id.org/semanticanalyser/>
        
        SELECT ?iri
        WHERE {
          GRAPH sa:system-graph {
            ?iri sa:hasAlias <XXXX>
          }
        }    
        """.replace("XXXX", thes_iri)
    r = send_query_to_db(q)
    if len(r) > 0:
        return r[0]["iri"]["value"]

    if thes_name is not None:
        # if not, see if we can name match the thesaurus
        q = """
            PREFIX dcat: <http://www.w3.org/ns/dcat#>
            PREFIX sa: <https://w3id.org/semanticanalyser/>
            PREFIX schema: <https://schema.org/>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            
            SELECT ?iri
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
            return r[0]["iri"]["value"]

    return None


def match_kw_to_kb(kw_text:str, kw_iri: str = None, thes_iri: str = None):
    if kw_iri is not None:
        if "https://www.ncei.noaa.gov/archive/accession/" in kw_iri:
            return kw_iri
        if "https://www.ncei.noaa.gov/archive/archive-management-system" in kw_iri:
            return kw_iri

    if "/" in kw_text:
        if kw_text.endswith("/"):
            kw_text = kw_text.split("/")[-2].strip()
        else:
            kw_text = kw_text.split("/")[-1].strip()
    if ">" in kw_text:
        kw_text = kw_text.split(">")[-1].strip()

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
                """.replace("ZZZ", kw_text)

    # print(q)
    r = send_query_to_db(q)

    if len(r) > 0:
        return r[0]["iri"]["value"]
    else:
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
            """.replace("ZZZ", kw_text)

        r = send_query_to_db(q)

        if len(r) > 0:
            return r[0]["iri"]["value"]

        return kw_text


def get_best_guess_kws(path_to_file_or_etree: Union[Path, etree]):
    et = path_to_file_or_etree if not isinstance(path_to_file_or_etree, Path) else etree.parse(path_to_file_or_etree)

    doc_iri, thesauri = get_thes_and_kws(et)

    replacement_iris = []

    for k, v in thesauri.items():
        if v["name"] != "":
            better_iri = match_thes_to_kb(k, v["name"])
            if better_iri is not None:
                replacement_iris.append((better_iri, k))

    for replacement_iri in replacement_iris:
        thesauri[replacement_iri[0]] = thesauri.pop(replacement_iri[1])

    for thesaurus, content in thesauri.items():
        thesaurus = None if thesaurus == "" else thesaurus

        improved_kws = []
        for kw in content["keywords"]:
            kw_iri = kw["value"] if kw["value"].startswith("http") else None
            val = match_kw_to_kb(kw["value"], kw_iri, thesaurus)
            # print(kw)
            # print(kw_iri)
            # print(val)

            theme = kw["theme"]
            improved_kws.append({
                "value": val,
                "theme": theme,
                "thesaurus": thesaurus,
                "original": kw["value"]
            })
        thesauri[thesaurus if thesaurus is not None else ""]["keywords"] = improved_kws

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
            kw_only.append((kw["theme"], kw["value"], kw["thesaurus"], kw["original"]))

    from operator import itemgetter
    # kw_only.sort(key=itemgetter(0, 1))

    from prettytable import PrettyTable
    table = PrettyTable()
    table.field_names = ["Theme", "Keyword", "Thesaurus", "Original"]
    for kw in kw_only:
        table.add_row([kw[0], kw[1], kw[2], kw[3]])
    print(table)


def convert_results_to_graph(thesauri: {}, doc_iri: str) -> Graph:
    g = Graph()
    g.add((URIRef(doc_iri), RDF.type, SDO.CreativeWork))
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
                    g.add((kw_iri, SDO.citation, Literal(kw["original"])))
            else:
                if not kw["original"].startswith("http"):
                    g.add((kw_iri, SDO.value, Literal(kw["original"])))

            if kw["thesaurus"] is not None:
                if kw["thesaurus"].startswith("http"):
                    g.add((kw_iri, SDO.inDefinedTermSet, URIRef(kw["thesaurus"])))
                else:
                    g.add((kw_iri, SDO.inDefinedTermSet, Literal(kw["thesaurus"])))

            g.add((URIRef(doc_iri), SDO.keywords, kw_iri))
    g.add((URIRef(doc_iri), RDF.type, SDO.CreativeWork))

    return g


def process_all_records(starting_record: None = 1, no_to_process: None = 100):
    start = time.process_time()

    records_dir = Path("/Users/nick/Work/bodc/sa-records")

    for idx, record in enumerate(open("records-index.txt").readlines()):
        if (idx + 1) < starting_record:
            continue

        record_path = records_dir.joinpath(record.strip())

        # Process the record
        doc_iri, thesauri = get_best_guess_kws(record_path)
        nt = convert_results_to_graph(thesauri, doc_iri).serialize(format="nt")
        open("record-keywords.2.nt", "a").write(nt + "\n\n")

        open("records-index-processed.txt", "a").write(record)

        if (idx + 2) - starting_record >= no_to_process:
            tdiff = time.process_time() - start
            print(f"End: {tdiff:.2f}s")
            avg = tdiff / no_to_process
            timing = f"No. {no_to_process} in {tdiff:.2f}s is {avg:.2f}s per record\n"
            open("records-processing-timing.txt", "a").write(timing)

            return


if __name__ == "__main__":
    # fn = "87ed5a7d-3de5-426c-be8d-5c1f1fe224ca.xml"  # none
    # fn = "f089a39e-a589-4b69-ab61-1ad4bb9a9d2a.xml"  #
    # fn = "cfee79a4-207c-4522-93fb-d230f9cff85e.xml"  # 7
    # fn = "4F44B923286F59CFEF35E1C5F4B9838008717C26.xml"
    # fn = "F339039A8B322A39F1AFEF986AC60E3276896D10.xml"
    # record_sample = [Path("/Users/nick/Work/bodc/sa-records/dashes") / fn]


    # record_sample = sample_records(3)

    #record_sample = [Path("/Users/nick/Work/bodc/sa-records/ifremer/0098a856-7401-46c2-9f7a-a2e5c0cf899c.xml")]
    #   * missing target application vocab online - https://sextant.ifremer.fr/geonetwork/srv/api/registries/vocabularies/local.target-application.myocean.target-application

    # record_sample = [Path("/Users/nick/Work/bodc/sa-records/ifremer/sdn-open:urn:SDN:CDI:LOCAL:696-486-486-ds07-4.xml")]
    # 100%

    # record_sample = [Path("/Users/nick/Work/bodc/sa-records/ifremer/f632d0d4-3373-43a4-a6be-d2109ebe0177.xml")]
    # 100%

    # record_sample = [Path("/Users/nick/Work/bodc/sa-records/ifremer/01f36842-927c-43a8-a531-f3c5096f3b34.xml")]
    # 21/37 - no thes for non-matches

    # record_sample = [Path("/Users/nick/Work/bodc/sa-records/ifremer/0c6e6b99-eaa6-41f7-b9f5-8a84d60b02b0.xml")]
    # 100s of Accession Records
    # GCMD Providers vocab missed <https://gcmd.earthdata.nasa.gov/kms/concept/086c68e5-1c94-4f2f-89d5-0453443ff249> - added
    # GCSM Science Keywords missing <https://gcmd.earthdata.nasa.gov/kms/concept/cd5a4729-ea4a-4ce1-8f5a-ec6a76d31055> - not yet added

    record_sample = sorted(Path("/Users/nick/Work/bodc/sa-records/ifremer").glob("*.xml"))
    count = 0
    for r in record_sample:
        doc_iri, thesauri = get_best_guess_kws(r)
        present_results(thesauri)
        with open(Path(__file__).parent / "ifremer-record-keywords.nt", "a") as f:
            f.write(convert_results_to_graph(thesauri, doc_iri).serialize(format="nt"))
            f.write("\n\n")
        count += 1
        print(f"record no. {count}")

    # process_all_records(5000, 1001)


