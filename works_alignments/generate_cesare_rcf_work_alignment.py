from pandas import DataFrame
import pandas as pd
import sparql_dataframe
import Levenshtein
from tqdm import tqdm

endpoint = "https://api.lod.uba.uva.nl/datasets/CREATE/ONSTAGE/services/ONSTAGE/sparql"

sparql_query = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
	PREFIX owl: <http://www.w3.org/2002/07/owl#>
	PREFIX xml: <http://www.w3.org/XML/1998/namespace>
	PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
	PREFIX ecrm: <http://erlangen-crm.org/current/>
	PREFIX crmpc: <http://dramabase/ontology/crm_property_classes/>
	PREFIX efrbroo: <http://erlangen-crm.org/efrbroo/>
	PREFIX dram: <http://dramabase/ontology/>
	PREFIX dc: <http://purl.org/dc/elements/1.1/>
  PREFIX dcterms: <http://purl.org/dc/terms/>
	PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX rcf: <http://rcf.logilab.fr/>



SELECT  ?script ?title ?performance  ?piece ?representation ?pieceTitre  WHERE {
 # ?sub ?pred ?obj .

  SERVICE <https://cesar2.huma-num.fr/sparql> {

    SELECT ?script ?maintitle ?subtitle ?performance  WHERE {

      ?performance a efrbroo:F31_Performance ;
    				ecrm:P4_has_time-span ?pts .
  		 ?pts dram:P82a_begin_of_the_begin ?start .


       ?performance dram:R66_included_performed_version_of ?script .
       ?script  dram:D102-1_has_preferred_title ?prefTitle .
  	   ?prefTitle ecrm:P148_has_component ?main .
  	   ?main a dram:DE35-1_Title-part_Main ;
           			rdfs:label ?maintitle .
      OPTIONAL {
  		?prefTitle 	ecrm:P148_has_component ?sub .
        ?sub 		a dram:DE35-2_Title-part_Subtitle ;
                   	rdfs:label ?subtitle .
      }
             FILTER(?start = \"##YEAR##\"^^xsd:date )

   #   BIND ( CONCAT(?maintitle, \" \", ?subtitle) as ?title)


  }



  }

  SERVICE <https://rcf-sparql.demo.logilab.fr/sparql/> {

    SELECT ?piece ?representation ?pieceTitre  WHERE {

    ?piece rcf:titre ?pieceTitre.
    ?representation rcf:aPourPiece ?piece.
    ?journee  rcf:aPourRepresentation ?representation ;
               rcf:aPourDate ?date.

   FILTER(?date = \"##YEAR##\"^^xsd:date )


    }


}
  BIND ( CONCAT(?maintitle, \" \", ?subtitle) as ?title)


}
"""


align_csv = []
seen = set()

for date in tqdm(pd.date_range("1681-09-30", "1684-09-30")):
    df = sparql_dataframe.get(endpoint, sparql_query.replace("##YEAR##", date.date().isoformat()))
    for index, row in df.iterrows():
        cesare_title = row.title
        rcf_title = row.pieceTitre
        distance = Levenshtein.ratio(cesare_title, rcf_title)
        if distance > 0.8 and row.pieceTitre not in seen:
            align_csv.append([
                row.piece, row.script, row.pieceTitre
            ])
            seen.add(row.pieceTitre)

DataFrame(align_csv, columns=['rcf_uri', 'cesare_uri', 'name']).to_csv("./rcf_cesare_alignments.csv", index=False)
