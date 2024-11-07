from enum import Enum
from typing import Optional, Union
from pathlib import Path
from xml.etree.ElementTree import indent

from lxml import etree
from utils import get_metadata_profile, make_record_iri, get_id, NAMESPACES, NAMESPACES_19139, NAMESPACES_19115_1, send_query_to_db, str_tidy
from hashlib import sha1


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
            f"gmx:Anchor",  # /
            namespaces=namespaces)

        improved_anchor_keywords = []
        for ak in anchor_keywords:
            link = ak.xpath("./@xlink:href", namespaces=namespaces)[0]
            txt = str_tidy(ak.text)
            if link == "http://inspire.ec.europa.eu/theme/of":
                improved_anchor_keywords.append(txt)
            else:
                improved_anchor_keywords.append(link)

        for x in text_keywords + improved_anchor_keywords:
            kw = x if x.startswith("http") else str_tidy(x)
            th = theme[0] if len(theme) > 0 else None
            kws.append((kw, th))
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

    return doc_iri, theses


def match_thes_to_kb(thes_iri: str, thes_name: str) -> {}:
    # see if we have an aliasFor IRI for this thesaurus
    q = """
        PREFIX sa: <http://w3id.org/semanticanalyser/>
        
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
            PREFIX sa: <http://w3id.org/semanticanalyser/>
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


def get_best_guess_kws(path_to_file_or_etree: Union[Path, etree]):
    et = path_to_file_or_etree if not isinstance(path_to_file_or_etree, Path) else etree.parse(path_to_file_or_etree)

    doc_iri, thesauri = get_thes_and_kws(et)

    replacement_iris = []

    for k, v in thesauri.items():
        better_iri = match_thes_to_kb(k, v["name"])
        if better_iri is not None:
            replacement_iris.append((better_iri, k))

    for replacement_iri in replacement_iris:
        thesauri[replacement_iri[0]] = thesauri.pop(replacement_iri[1])

    return doc_iri, thesauri


if __name__ == "__main__":
    fn = "87ed5a7d-3de5-426c-be8d-5c1f1fe224ca.xml"  # none
    fn = "f089a39e-a589-4b69-ab61-1ad4bb9a9d2a.xml"  #
    # fn = "cfee79a4-207c-4522-93fb-d230f9cff85e.xml"  # 7
    test_file = Path(__file__).parent.parent.resolve() / "tests" / "data" / fn

    doc_iri, thesauri = get_best_guess_kws(test_file)
    import json
    print(json.dumps(thesauri, indent=4))


    # for k, v in th.items():
    #     print(k)
    #     match = match_thes_to_kb(v["name"])
    #     if len(match) > 0:  # ordered so highest value at position 0
    #         if int(match[0]["w"]["value"]) > 8:  # 9 or 10 is alt/prefLabel exact match only
    #             print(match[0]["iri"]["value"])