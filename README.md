# ONLINE-ONLY TUTORIAL

## this is intended to give any registered user the chance to try Diagnosticator before deploy it locally

### to create the static files from the analyzed ones
```
cd ${DXCATOR_DIR}
source venv/bin/activate

from convert_VCF_REDIS import VCF2REDIS

ASILO_DIR = '/home/enrico/Downloads/diagnosticator-prove/analisi_result'

var_dict, sample_dict, gene_dict = VCF2REDIS( ASILO_DIR )

import json
with open('/home/enrico/Downloads/diagnosticator-prove/var_dict.json', 'w') as fp:
    json.dump(var_dict, fp)

with open('/home/enrico/Downloads/diagnosticator-prove/sample_dict.json', 'w') as fp:
    json.dump(sample_dict, fp)

with open('/home/enrico/Downloads/diagnosticator-prove/gene_dict.json', 'w') as fp:
    json.dump(gene_dict, fp)
```






























###ENDc
