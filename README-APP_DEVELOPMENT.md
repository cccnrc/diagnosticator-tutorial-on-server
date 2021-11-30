### I frequenlty get an internal error of no SAMPLES key in variant_dict on variant_page
redis-cli -h 127.0.0.1 -p 6377
select 2
type var:chr1-17022685-G-A
hgetall var:chr1-17022685-G-A CHARS

VOL='/var/lib/docker/volumes/diagnosticator-local-simple-algorithm-development-02-nomysql_DX-UPLOAD/_data'
cat $VOL/analisi_result/var_results/prova.var.chr* | gawk -F'\t' '$2==77293763'



python3.9
from convert_VCF_REDIS import VCF2REDIS

ASILO_OUT_DIR = '/var/lib/docker/volumes/diagnosticator-local-simple-algorithm-development-02-nomysql_DX-UPLOAD/_data/analisi_result'
ASILO_OUT_DIR = '/home/enrico/columbia/diagnosticator-AWS/diagnosticator-local-simple-ALGORITHM-DEVELOPMENT-02-noMySQL-TUTORIAL/upload/analisi_result'
var_dict, sample_dict, gene_dict = VCF2REDIS( ASILO_OUT_DIR )

var_dict['chr20-19974948-CA-C']

import redis_functions
vi = redis_functions.redis_dict_insert( url = "redis://127.0.0.1:6377", database = 2, d_dict = var_dict, key_prefix = 'var', pwd = None )
si = redis_functions.redis_dict_insert( url = "redis://127.0.0.1:6377", database = 2, d_dict = sample_dict, key_prefix = 'sam', pwd = None )
gi = redis_functions.redis_dict_insert( url = "redis://127.0.0.1:6377", database = 2, d_dict = gene_dict, key_prefix = 'gen', pwd = None )


### DICT return from REDIS
import redis_functions
REDIS_URL = "redis://127.0.0.1:6377"

aa = redis_functions.redis_dict_return( REDIS_URL, 2, 'var', key_value = None, pwd = None )

from app.variant_functions import get_known_variants_from_DB, get_all_known_variants

known_dict = get_known_variants_from_DB()
user_var = redis_functions.redis_dict_return( url = current_app.config['REDIS_URL'], database = 2, key_prefix = 'var' )



### this is to login as enrico0
from app.models import User
user = User.query.first()
request_ctx = app.test_request_context()
request_ctx.push()
from flask_login import login_user
login_user(user)
from flask_login import current_user
current_user.id == user.id

### this is to login as enrico1
from app.models import User
user = User.query.all()[1]
request_ctx = app.test_request_context()
request_ctx.push()
from flask_login import login_user
login_user(user)
from flask_login import current_user
current_user.id == user.id

known_server_dict = get_all_known_variants()









### ENDC
