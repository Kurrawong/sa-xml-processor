# Semantic Analyser XML Records processor

This repository contains Python code used to process XML metadata records offline, to determine what concepts from vocabularies they are quoting within keyword fields.

> [!NOTE]  
> This repository relies on a Knowledge Base established for the Semantic Analyser tool and establishment data and scripts for that are held within the [SA KB Enhancements](https://github.com/Kurrawong/sa-kb-enhancements) repository which builds on the [data dump of the SA' original KB](https://github.com/Kurrawong/fair-ease-matcher/tree/bodc/triple_store_dump_2024_07_22/data/fuseki_triple_store_dump_2024_07_22).



## Defined Term Modelling

### Indicating a keyword was found in a particular resource

```
<{METADATA-RECORD-IRI}>
    a schema:CreativeWork ;
    schema:keywords
        <{KNOWN-KW-01-IRI}> ,
        <{KNOWN-KW-02-IRI}> ,
        ...
        [
            a schema:DefinedTerm ;
            schema:value "{UNKNOWN-KW-LITERAL-03}" ;
        ] ,
        [
            a schema:DefinedTerm ;
            schema:value "{UNKNOWN-KW-LITERAL-04}" ;
        ] ,
        ...
.       
```

### Indicating the original resource value used in keyword matching

```
<http://vocab.nerc.ac.uk/collection/P07/current/CFSN0041/>
    a schema:DefinedTerm ;
    schema:citation
        [
            schema:isBasedOn <http://example.com/record/0048F0DF85529BD09AA6FD32D8BD6DBB0F34AD89> ;
            schema:value "wind_to_direction" ;
        ] ;
.
```

In the above example, the keyword `<http://vocab.nerc.ac.uk/collection/P07/current/CFSN0041/>` was assigned to the resource `<http://example.com/record/0048F0DF85529BD09AA6FD32D8BD6DBB0F34AD89>` based on the original literal keyword of `"wind_to_direction"`.

A keyword may be indicated to have been assigned to multiple resources based on the same, or different, original literal values.

### Indicating a preferred keyword has been assigned

replacee...