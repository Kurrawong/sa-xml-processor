grep "https://sextant.ifremer.fr/geonetwork/srv/api/registries/vocabularies/" /Users/nick/Work/bodc/sa-records/dashes/* > ifremer-vocs.txt

find '/Users/nick/Work/bodc/sa-records/dashes/' -name "*.xml" -exec grep -H "https://sextant.ifremer.fr/geonetwork/srv/api/registries/vocabularies/" {} \;