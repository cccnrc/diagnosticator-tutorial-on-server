import redis_functions
from flask import current_app


def get_classes_dict ():
    '''
        this is to have classes and description declared only once
    '''
    d = ({
            'AC' : { 'display' : 'DF', 'description' : 'Positive Diagnostic Finding', 'btn-class' : 'success', 'btn-style' : 'opacity: 1;' },
            'SE' : { 'display' : 'SF', 'description' : 'Secondary Diagnostic Finding', 'btn-class' : 'primary', 'btn-style' : 'opacity: 1;' },
            'RE' : { 'display' : 'NE', 'description' : 'Negative', 'btn-class' : 'danger', 'btn-style' : 'opacity: 1;' },
            'US' : { 'display' : 'US', 'description' : 'Unknown Significance', 'btn-class' : 'warning', 'btn-style' : 'opacity: 1;' },
            'AR' : { 'display' : 'Dr', 'description' : 'Positive Diagnostic Finding (temporary)', 'btn-class' : 'success', 'btn-style' : 'opacity: 0.5;' },
            'SR' : { 'display' : 'Sr', 'description' : 'Secondary Diagnostic Finding (temporary)', 'btn-class' : 'primary', 'btn-style' : 'opacity: 0.5;' },
            'RR' : { 'display' : 'Nr', 'description' : 'Negative (temporary)', 'btn-class' : 'danger', 'btn-style' : 'opacity: 0.5;' },
            'UR' : { 'display' : 'Ur', 'description' : 'Unknown Significance (temporary)', 'btn-class' : 'warning', 'btn-style' : 'opacity: 0.5;' },
            'IP' : { 'display' : 'IP', 'description' : 'In Progress', 'btn-class' : 'basic', 'btn-style' : 'opacity: 1;' },
            'NA' : { 'display' : 'restore', 'description' : 'default', 'btn-class' : 'default', 'btn-style' : 'opacity: 1;' }
    })
    return(d)


def get_ACMG_classes_dict():
    d = ({
            'P' : { 'display' : 'Pathogenic', 'description' : 'Pathogenic Variant', 'btn-class' : 'danger', 'btn-style' : 'opacity: 1;' },
            'LP' : { 'display' : 'Likely Pathogenic', 'description' : 'Likely Pathogenic Variant', 'btn-class' : 'danger', 'btn-style' : 'opacity: 0.5;' },
            'B' : { 'display' : 'Benign', 'description' : 'Benign Variant', 'btn-class' : 'success', 'btn-style' : 'opacity: 1;' },
            'LB' : { 'display' : 'Likely Benign', 'description' : 'Likely Benign Variant', 'btn-class' : 'success', 'btn-style' : 'opacity: 0.5;' },
            'US' : { 'display' : 'Unknown Significance', 'description' : 'Variant of Unknown Significance', 'btn-class' : 'warning', 'btn-style' : 'opacity: 1;' }
    })
    return(d)


def get_ACMG_strength_dict():
    d = ({
            'pvs1' : { 'display' : 'PVS1', 'description' : 'Pathogenic Very Strong 1: Null variant (nonsense, frameshift, canonical ±1 or 2 splice sites, initiation codon, single or multiexon deletion) in a gene where LOF is a known mechanism of disease', 'btn-class' : 'danger', 'btn-style' : 'opacity: 1;', 'subclass' : [ 'VS', 'S', 'M', 'P', 'NA'] },
            'ps1' : { 'display' : 'PS1', 'description' : 'Pathogenic Strong 1: Same amino acid change as a previously established pathogenic variant regardless of nucleotide change', 'btn-class' : 'danger', 'btn-style' : 'opacity: 0.8;', 'subclass' : [ 'VS', 'S', 'M', 'P', 'NA'] },
            'ps2' : { 'display' : 'PS2', 'description' : 'Pathogenic Strong 2: De novo (both maternity and paternity confirmed) in a patient with the disease and no family history', 'btn-class' : 'danger', 'btn-style' : 'opacity: 0.8;', 'subclass' : [ 'VS', 'S', 'M', 'P', 'NA'] },
            'ps3' : { 'display' : 'PS3', 'description' : 'Pathogenic Strong 3: Well-established in vitro or in vivo functional studies supportive of a damaging effect on the gene or gene product', 'btn-class' : 'danger', 'btn-style' : 'opacity: 0.8;', 'subclass' : [ 'VS', 'S', 'M', 'P', 'NA'] },
            'ps4' : { 'display' : 'PS4', 'description' : 'Pathogenic Strong 4: The prevalence of the variant in affected individuals is significantly increased compared with the prevalence in controls', 'btn-class' : 'danger', 'btn-style' : 'opacity: 0.8;', 'subclass' : [ 'VS', 'S', 'M', 'P', 'NA'] },
            'pm1' : { 'display' : 'PM1', 'description' : 'Pathogenic Moderate 1: Located in a mutational hot spot and/or critical and well-established functional domain (e.g., active site of an enzyme) without benign variation', 'btn-class' : 'danger', 'btn-style' : 'opacity: 0.6;', 'subclass' : [ 'VS', 'S', 'M', 'P', 'NA'] },
            'pm2' : { 'display' : 'PM2', 'description' : 'Pathogenic Moderate 2: Absent from controls (or at extremely low frequency if recessive) in Exome Sequencing Project, 1000 Genomes Project, or Exome Aggregation Consortium', 'btn-class' : 'danger', 'btn-style' : 'opacity: 0.6;', 'subclass' : [ 'VS', 'S', 'M', 'P', 'NA'] },
            'pm3' : { 'display' : 'PM3', 'description' : 'Pathogenic Moderate 3: For recessive disorders, detected in trans with a pathogenic variant', 'btn-class' : 'danger', 'btn-style' : 'opacity: 0.6;', 'subclass' : [ 'VS', 'S', 'M', 'P', 'NA'] },
            'pm4' : { 'display' : 'PM4', 'description' : 'Pathogenic Moderate 4: Protein length changes as a result of in-frame deletions/insertions in a non-repeat region or stop-loss variants', 'btn-class' : 'danger', 'btn-style' : 'opacity: 0.6;', 'subclass' : [ 'VS', 'S', 'M', 'P', 'NA'] },
            'pm5' : { 'display' : 'PM5', 'description' : 'Pathogenic Moderate 5: Novel missense change at an amino acid residue where a different missense change determined to be pathogenic has been seen before', 'btn-class' : 'danger', 'btn-style' : 'opacity: 0.6;', 'subclass' : [ 'VS', 'S', 'M', 'P', 'NA'] },
            'pm6' : { 'display' : 'PM6', 'description' : 'Pathogenic Moderate 6: Assumed de novo, but without confirmation of paternity and maternity', 'btn-class' : 'danger', 'btn-style' : 'opacity: 0.6;', 'subclass' : [ 'VS', 'S', 'M', 'P', 'NA'] },
            'pp1' : { 'display' : 'PP1', 'description' : 'Pathogenic Supporting  1: Cosegregation with disease in multiple affected family members in a gene definitively known to cause the disease', 'btn-class' : 'danger', 'btn-style' : 'opacity: 0.40;', 'subclass' : [ 'VS', 'S', 'M', 'P', 'NA'] },
            'pp2' : { 'display' : 'PP2', 'description' : 'Pathogenic Supporting  2: Missense variant in a gene that has a low rate of benign missense variation and in which missense variants are a common mechanism of disease', 'btn-class' : 'danger', 'btn-style' : 'opacity: 0.40;', 'subclass' : [ 'VS', 'S', 'M', 'P', 'NA'] },
            'pp3' : { 'display' : 'PP3', 'description' : 'Pathogenic Supporting  3: Pathogenic computational verdict', 'btn-class' : 'danger', 'btn-style' : 'opacity: 0.40;', 'subclass' : [ 'VS', 'S', 'M', 'P', 'NA'] },
            'pp4' : { 'display' : 'PP4', 'description' : 'Pathogenic Supporting  4: Patient’s phenotype or family history is highly specific for a disease with a single genetic etiology', 'btn-class' : 'danger', 'btn-style' : 'opacity: 0.40;', 'subclass' : [ 'VS', 'S', 'M', 'P', 'NA'] },
            'pp5' : { 'display' : 'PP5', 'description' : 'Pathogenic Supporting  5: Reputable source recently reports variant as pathogenic, but the evidence is not available to the laboratory to perform an independent evaluation', 'btn-class' : 'danger', 'btn-style' : 'opacity: 0.40;', 'subclass' : [ 'VS', 'S', 'M', 'P', 'NA'] },
            'ba1' : { 'display' : 'BA1', 'description' : 'Benign Absolute 1: Allele frequency is >5% in Exome Sequencing Project, 1000 Genomes Project, or Exome Aggregation Consortium', 'btn-class' : 'success', 'btn-style' : 'opacity: 1;', 'subclass' : [ 'BA', 'BP', 'NA'] },
            'bs1' : { 'display' : 'BS1', 'description' : 'Benign Strong 1: Allele frequency is greater than expected for disorder', 'btn-class' : 'success', 'btn-style' : 'opacity: 0.8;', 'subclass' : [ 'BS', 'BP', 'NA'] },
            'bs2' : { 'display' : 'BS2', 'description' : 'Benign Strong 2: Observed in a healthy adult individual for a recessive (homozygous), dominant (heterozygous), or X-linked (hemizygous) disorder, with full penetrance expected at an early age', 'btn-class' : 'success', 'btn-style' : 'opacity: 0.8;', 'subclass' : [ 'BS', 'BP', 'NA'] },
            'bs3' : { 'display' : 'BS3', 'description' : 'Benign Strong 3: Well-established in vitro or in vivo functional studies show no damaging effect on protein function or splicing', 'btn-class' : 'success', 'btn-style' : 'opacity: 0.8;', 'subclass' : [ 'BS', 'BP', 'NA'] },
            'bs4' : { 'display' : 'BS4', 'description' : 'Benign Strong 4: Lack of segregation in affected members of a family', 'btn-class' : 'success', 'btn-style' : 'opacity: 0.8;', 'subclass' : [ 'BS', 'BP', 'NA'] },
            'bp1' : { 'display' : 'BP1', 'description' : 'Benign Supporting  1: Missense variant in a gene for which primarily truncating variants are known to cause disease', 'btn-class' : 'success', 'btn-style' : 'opacity: 0.40;', 'subclass' : [ 'BS', 'BP', 'NA'] },
            'bp2' : { 'display' : 'BP2', 'description' : 'Benign Supporting  2: Observed in trans with a pathogenic variant for a fully penetrant dominant gene/disorder or observed in cis with a pathogenic variant in any inheritance pattern', 'btn-class' : 'success', 'btn-style' : 'opacity: 0.40;', 'subclass' : [ 'BS', 'BP', 'NA'] },
            'bp3' : { 'display' : 'BP3', 'description' : 'Benign Supporting  3: In-frame deletions/insertions in a repetitive region without a known function', 'btn-class' : 'success', 'btn-style' : 'opacity: 0.40;', 'subclass' : [ 'BS', 'BP', 'NA'] },
            'bp4' : { 'display' : 'BP4', 'description' : 'Benign Supporting  4: Multiple lines of computational evidence suggest no impact on gene or gene product', 'btn-class' : 'success', 'btn-style' : 'opacity: 0.40;', 'subclass' : [ 'BS', 'BP', 'NA'] },
            'bp5' : { 'display' : 'BP5', 'description' : 'Benign Supporting  5: Variant found in a case with an alternate molecular basis for disease', 'btn-class' : 'success', 'btn-style' : 'opacity: 0.40;', 'subclass' : [ 'BS', 'BP', 'NA'] },
            'bp6' : { 'display' : 'BP6', 'description' : 'Benign Supporting  6: Reputable source recently reports variant as benign, but the evidence is not available to the laboratory to perform an independent evaluation', 'btn-class' : 'success', 'btn-style' : 'opacity: 0.40;', 'subclass' : [ 'BS', 'BP', 'NA'] },
            'bp7' : { 'display' : 'BP7', 'description' : 'Benign Supporting  7: A synonymous (silent) variant for which splicing prediction algorithms predict no impact to the splice consensus sequence nor the creation of a new splice site AND the nucleotide is not highly conserved', 'btn-class' : 'success', 'btn-style' : 'opacity: 0.40;', 'subclass' : [ 'BS', 'BP', 'NA'] }
    })
    return(d)


def get_ACMG_subclass_dict():
    d = ({
            'VS' : { 'display' : 'VS', 'description' : 'Very Strong', 'btn-class' : 'danger', 'btn-style' : 'opacity: 1;' },
            'S' : { 'display' : 'S', 'description' : 'Strong', 'btn-class' : 'danger', 'btn-style' : 'opacity: 0.80;' },
            'M' : { 'display' : 'M', 'description' : 'Moderate', 'btn-class' : 'danger', 'btn-style' : 'opacity: 0.60;' },
            'P' : { 'display' : 'P', 'description' : 'Supporting', 'btn-class' : 'danger', 'btn-style' : 'opacity: 0.40;' },
            'BA' : { 'display' : 'BA', 'description' : 'Benign Absolute', 'btn-class' : 'success', 'btn-style' : 'opacity: 1;' },
            'BS' : { 'display' : 'BS', 'description' : 'Benign Strong', 'btn-class' : 'success', 'btn-style' : 'opacity: 0.80;' },
            'BP' : { 'display' : 'BP', 'description' : 'Benign Supporting', 'btn-class' : 'success', 'btn-style' : 'opacity: 0.40;' },
            'NA' : { 'display' : 'restore', 'description' : 'Restore', 'btn-class' : 'default', 'btn-style' : 'opacity: 1;' }
    })
    return(d)






################################################################



def getSamplesHTMLdict( sample_dict, variant_dict ):
    '''
        this takes sample and variant dict and returns a dict with the values I need for HTML display
    '''
    result_dict = {}
    for sample_name, s_dict in sample_dict.items() :
        varN = 0
        pN = 0
        kN = 0
        for var_name in s_dict['varGT'] :
            varN += 1
            if variant_dict[ var_name ]['ACMG']['ACMG'] == 'P' or variant_dict[ var_name ]['ACMG']['ACMG'] == 'LP':
                pN += 1
            if 'KNOWN' in variant_dict[var_name]:
                if 'P' in variant_dict[var_name]['KNOWN'] or 'LP' in variant_dict[var_name]['KNOWN']:
                    kN += 1
        try:
            status = s_dict['STATUS']['status']
        except:
            status = 'NA'
        result_dict.update({ sample_name : { 'varN' : varN, 'pN' : pN, 'kN' : kN, 'status' : status } })
    return( result_dict )




def adaptVARdict( var_dict ):
    '''
        this makes adaptments to variant dictionary
        - HGVSc
        - HGVSp
    '''
    if 'ACMG' in var_dict :
        var_dict['ACMG'].update({ 'score' : assignACMGscore( var_dict )})
    if 'CHARS' in var_dict:
        if 'HGVSc' in var_dict['CHARS']:
            var_dict['CHARS']['HGVSc'] = extractHGVS( var_dict['CHARS']['HGVSc'] )
        if 'HGVSp' in var_dict['CHARS']:
            var_dict['CHARS']['HGVSp'] = extractHGVS( var_dict['CHARS']['HGVSp'] )
    return( var_dict )


def getSampleVariants( sample_dict ):
    '''
        this extracts variants characteristics for all variants in a specific sample
        returning a dict K=var_name : V=chars
    '''
    results_dict = {}
    for var_name in sample_dict[ 'varGT' ]:
        var_dict = redis_functions.redis_dict_return( url = current_app.config['REDIS_URL'], database = 2, key_prefix = 'var', key_value = var_name  )
        var_dict = adaptVARdict( var_dict )
        results_dict.update({ var_name : var_dict })
    return( results_dict )


def getSampleVariantsJSON( sample_dict, variant_dict ):
    '''
        this extracts variants characteristics for all variants in a specific sample
        returning a dict K=var_name : V=chars
    '''
    results_dict = {}
    for var_name in sample_dict[ 'varGT' ]:
        var_dict = variant_dict[var_name]
        var_dict = adaptVARdict( var_dict )
        results_dict.update({ var_name : var_dict })
    return( results_dict )


def check_status_in_dict( d ):
    '''
        this allows to add NA as status in case it is not present in dict
    '''
    if not 'status' in d:
        d.update({ 'status' : 'NA'  })
    return(d)



def orderDictByScore( sampleVar_dict ):
    '''
        this function orders variants in the sampleVar_dict by their ACMG calculated score
    '''
    result_dict = {}
    final_dict = {}
    '''
    for k,v in sampleVar_dict.items():
        print(k)
        print(v)
    '''
    for var_name,var_dict in sampleVar_dict.items() :
        if 'ACMG' in var_dict:
            if 'score' in var_dict['ACMG']:
                result_dict.update({ var_name : var_dict['ACMG']['score'] })
    k_list = []
    for k in sorted( result_dict, key = result_dict.get, reverse = True ):
        k_list.append(k)
    for k in k_list:
        final_dict.update({ k : sampleVar_dict[k] })
    return( final_dict )


def scorePoint( value ):
    if value == 'VS':
        score = 4
    elif value == 'S':
        score = 3
    elif value == 'M':
        score = 2
    elif value == 'P':
        score = 1
    elif value == 'BA':
        score = -10
    elif value == 'BS':
        score = -3
    elif value == 'BP':
        score = -1
    else:
        score = 0
    return( score )


def assignACMGscore( var_dict ):
    keys = list(get_ACMG_strength_dict().keys())
    p_keys = [ k for k in keys if k.startswith('p') ]
    b_keys = [ k for k in keys if k.startswith('b') ]
    score = 0
    if 'ACMG' in var_dict:
        if 'ACMG' in var_dict['ACMG'] :
            if var_dict['ACMG']['ACMG'] == 'B':
                score -= 20
            elif var_dict['ACMG']['ACMG'] == 'LB':
                score -= 10
            elif var_dict['ACMG']['ACMG'] == 'LP':
                score += 10
            elif var_dict['ACMG']['ACMG'] == 'P':
                score += 20
        for k,v in var_dict['ACMG'].items():
            if k in p_keys or k in b_keys:
                score += scorePoint( var_dict['ACMG'][k] )
    return( score )

def extractHGVS( h ):
    if ':' in h:
        return( h.split(':')[-1] )
    else:
        return(h)




def change_status( url, key_prefix, key_value, new_status, database = 2, pwd = None ):
    allowed_status = list(get_classes_dict().keys())
    if new_status not in allowed_status:
        print( "change_status() !!! ERROR !!! Modifying {0}:{1} to {2} status is not in allowed list: {3}".format( key_name, key_value, new_status, allowed_status ) )
        return( False )
    if addDictElement( key_prefix = key_prefix, key_value = key_value, subdict_name = 'STATUS', element_name = 'status', element_value = new_status, pwd = pwd, url = current_app.config['REDIS_URL']  ):
        return( True )
    return(False)


def addDictElement( url, key_prefix, key_value, subdict_name, element_name, element_value, database = 2, pwd = None ):
    '''
        this allows to add/update an element to a row entry in mongoDB
    '''
    if redis_functions.redis_add_dict_element( url, database, key_prefix, key_value, subdict_name, element_name, element_value, pwd ):
        return( True )
    return( False )





def get_samples_VAR_status( variant_name, variant_dict ):
    '''
        this functions retrieves the variant status of each specific sample from the mongoDB
    '''
    sampleVARstatus_dict = {}
    for sample_name in variant_dict[ 'SAMPLES' ]:
        sample_dict = redis_functions.redis_dict_return( url = current_app.config['REDIS_URL'], database = 2, key_prefix = 'sam', key_value = sample_name )
        try:
            sampleVARstatus = sample_dict[ 'STATUS' ][ variant_name ]
        except:
            print( "get_samples_VAR_status() WARNING: assigning default NA status to sample {0} for variant {1}".format( sample_name, variant_name ) )
            sampleVARstatus = 'NA'
        sampleVARstatus_dict.update({ sample_name : sampleVARstatus })
    return( sampleVARstatus_dict )


def get_samples_VAR_status_JSON( variant_name, variant_dict, samples_dict ):
    '''
        this functions retrieves the variant status of each specific sample from the mongoDB
    '''
    sampleVARstatus_dict = {}
    for sample_name in variant_dict[ 'SAMPLES' ]:
        sample_dict = samples_dict[sample_name]
        try:
            sampleVARstatus = sample_dict[ 'STATUS' ][ variant_name ]
        except:
            # print( "get_samples_VAR_status() WARNING: assigning default NA status to sample {0} for variant {1}".format( sample_name, variant_name ) )
            sampleVARstatus = 'NA'
        sampleVARstatus_dict.update({ sample_name : sampleVARstatus })
    return( sampleVARstatus_dict )



def removeKdict( k, d ):
    if k in d:
        d.pop(k)
    return(d)

def arrangeVARdict( variant_dict ):
    '''
        this is to modify variant dict in order to make it displayable
    '''
    k_order = ([
        'CHARS',
        'AF',
        'CLINVAR',
        'KNOWN',
        'PRED',
        'ACMG',
        'SAMPLES'
    ])
    if 'CHARS' in variant_dict:
        variant_dict = removeKdict( 'Allele', variant_dict )
        if 'HGVSc' in variant_dict['CHARS']:
            variant_dict['CHARS']['HGVSc'] = extractHGVS( variant_dict['CHARS']['HGVSc'] )
        if 'HGVSp' in variant_dict['CHARS']:
            variant_dict['CHARS']['HGVSp'] = extractHGVS( variant_dict['CHARS']['HGVSp'] )
    variant_dict['ACMG'] = order_ACMG_dict( variant_dict['ACMG'] )
    r = {}
    for k in k_order:
        if k in variant_dict:
            r.update({ k : variant_dict[k] })
    return( r )



def order_ACMG_dict(d):
    '''
        this is to render ACMG dict in order
    '''
    r = {}
    if 'ACMG' in d:
        r.update({ 'ACMG' : d['ACMG'] })
    for k in get_ACMG_strength_dict():
        if k in d:
            r.update({ k : d[k] })
    return(r)




def addACMGkeys( d ):
    keys = list(get_ACMG_strength_dict().keys())
    p_keys = [ k for k in keys if k.startswith('p') ]
    b_keys = [ k for k in keys if k.startswith('b') ]
    new_d = {}
    for p in p_keys:
        if p not in d:
            new_d.update({ p : 'NA' })
        else:
            new_d.update({ p : d[p] })
    for b in b_keys:
        if b not in d:
            new_d.update({ b : 'NA' })
        else:
            new_d.update({ b : d[b] })
    return( new_d )





def calculateACMG( varACMG_dict ):
    keys = list(get_ACMG_strength_dict().keys())
    p_keys = [ k for k in keys if k.startswith('p') ]
    b_keys = [ k for k in keys if k.startswith('b') ]
    varACMG_P_categories_dict = ({
            'VS' : 0,
            'S' : 0,
            'M' : 0,
            'P' : 0
    })
    varACMG_B_categories_dict = ({
            'BA' : 0,
            'BS' : 0,
            'BP' : 0
    })
    for p in p_keys:
        if p in varACMG_dict :
            if varACMG_dict[p] == 'NA' :
                continue
            varACMG_P_categories_dict[ varACMG_dict[p] ] += 1
    for b in b_keys:
        if b in varACMG_dict :
            if varACMG_dict[b] == 'NA' :
                continue
            varACMG_B_categories_dict[ varACMG_dict[b] ] += 1

    ### recalculate ACMG overall
    overallACMG = 'US'
    overallACMGpatho = 0
    overallACMGbenign = 0

    if varACMG_B_categories_dict[ 'BA' ] > 0:
        varACMG_dict['ACMG'] = 'B'
        return( 'B' )
    elif varACMG_B_categories_dict[ 'BS' ] > 1:
        overallACMG = 'B'
    elif varACMG_B_categories_dict[ 'BS' ] > 0 and varACMG_B_categories_dict[ 'BP' ] > 0:
        overallACMG = 'LB'

    if overallACMG == 'B' or overallACMG == 'LB' :
        overallACMGbenign = 1

    # PATHO
    if varACMG_P_categories_dict[ 'S' ] < 2:
        if varACMG_P_categories_dict[ 'VS' ] > 0 and varACMG_P_categories_dict[ 'S' ] > 0 :
            overallACMG = 'P'
        elif varACMG_P_categories_dict[ 'VS' ] > 0 and varACMG_P_categories_dict[ 'M' ] > 1 :
            overallACMG = 'P'
        elif varACMG_P_categories_dict[ 'VS' ] > 0 and varACMG_P_categories_dict[ 'M' ] > 0 and varACMG_P_categories_dict[ 'P' ] > 0 :
            overallACMG = 'P'
        elif varACMG_P_categories_dict[ 'VS' ] > 0 and varACMG_P_categories_dict[ 'P' ] > 1 :
            overallACMG = 'P'
        ### this is the new one added
        elif varACMG_P_categories_dict[ 'VS' ] > 0 and varACMG_P_categories_dict[ 'P' ] > 0 :
            overallACMG = 'P'
        elif varACMG_P_categories_dict[ 'S' ] > 1 :
            overallACMG = 'P'
        elif varACMG_P_categories_dict[ 'S' ] > 1 and varACMG_P_categories_dict[ 'M' ] > 2 :
            overallACMG = 'P'
        elif varACMG_P_categories_dict[ 'S' ] > 0 and varACMG_P_categories_dict[ 'M' ] > 1 and varACMG_P_categories_dict[ 'P' ] > 2 :
            overallACMG = 'P'
        elif varACMG_P_categories_dict[ 'S' ] > 0 and varACMG_P_categories_dict[ 'M' ] > 0 and varACMG_P_categories_dict[ 'P' ] > 3 :
            overallACMG = 'P'
        elif varACMG_P_categories_dict[ 'VS' ] > 0 and varACMG_P_categories_dict[ 'M' ] > 0 :
            overallACMG = 'LP'
        elif varACMG_P_categories_dict[ 'S' ] > 0 and varACMG_P_categories_dict[ 'M' ] > 0 and varACMG_P_categories_dict[ 'M' ] < 3 :
            overallACMG = 'P'
        elif varACMG_P_categories_dict[ 'S' ] > 0 and varACMG_P_categories_dict[ 'P' ] > 1 :
            overallACMG = 'LP'
        elif varACMG_P_categories_dict[ 'M' ] > 2 :
            overallACMG = 'LP'
        elif varACMG_P_categories_dict[ 'M' ] > 1 and varACMG_P_categories_dict[ 'P' ] > 1 :
            overallACMG = 'LP'
        elif varACMG_P_categories_dict[ 'M' ] > 0 and varACMG_P_categories_dict[ 'P' ] > 3 :
            overallACMG = 'LP'
    else:
        overallACMG = 'P'

    if overallACMG == 'P' or overallACMG == 'LP' :
        overallACMGpatho = 1

    ### if both P and B then US
    if overallACMGpatho > 0 and overallACMGbenign > 0 :
        overallACMG = 'US'

    varACMG_dict['ACMG'] = overallACMG

    return( overallACMG )









def arrange_gene_dict( gene_dict ):
    if '_id' in gene_dict:
        gene_dict.pop('_id')
    if 'gen' in gene_dict:
        gene_dict.pop('gen')
    return( gene_dict )

def extract_geneHTML_dict( genes_dict, variant_dict ) :
    d = {}
    for gene, gene_dict in genes_dict.items():
        varN = 0
        pN = 0
        if 'var' in gene_dict:
            for var_name in gene_dict['var']:
                varN += 1
                if variant_dict[ var_name ]['ACMG']['ACMG'] == 'P' or variant_dict[ var_name ]['ACMG']['ACMG'] == 'LP':
                    pN += 1
        d.update({ gene : { 'varN' : varN, 'pN' : pN } })
    return( d )



def get_all_genes_dict():
    genes_dict = redis_functions.redis_dict_return( url = current_app.config['REDIS_URL'], database = 2, key_prefix = 'gen' )
    variant_dict = redis_functions.redis_dict_return( url = current_app.config['REDIS_URL'], database = 2, key_prefix = 'var' )
    geneHTML_dict = extract_geneHTML_dict( genes_dict, variant_dict )
    return( geneHTML_dict )

def get_all_genes_dict_JSON( gene_dict, variant_dict ):
    # genes_dict = redis_functions.redis_dict_return( url = current_app.config['REDIS_URL'], database = 2, key_prefix = 'gen' )
    # variant_dict = redis_functions.redis_dict_return( url = current_app.config['REDIS_URL'], database = 2, key_prefix = 'var' )
    geneHTML_dict = extract_geneHTML_dict( gene_dict, variant_dict )
    return( geneHTML_dict )





def update_sampleVAR_status( variant_name, sample_name, new_status ) :
    '''
        this function is intended to update the status of a variant for a specific sample
    '''
    variant_dict = redis_functions.redis_dict_return( url = current_app.config['REDIS_URL'], database = 2, key_prefix = 'var', key_value = variant_name )
    sample_dict = redis_functions.redis_dict_return( url = current_app.config['REDIS_URL'], database = 2, key_prefix = 'sam', key_value = sample_name )
    sampleVARstatus_dict = get_samples_VAR_status( variant_name, variant_dict )
    if sample_name in sampleVARstatus_dict:
        if sampleVARstatus_dict[ sample_name ] == new_status:
            print("update_sampleVAR_status(): variant: {0}, sample: {1}, status is already {2}".format( variant_name, sample_name, new_status ) )
            return( True )
    if redis_functions.redis_add_dict_element( url = current_app.config['REDIS_URL'], database = 2, key_prefix = 'sam', key_value = sample_name, subdict_name = 'STATUS', element_name = variant_name, element_value = new_status, pwd = None ):
        # mongodb_functions.updateDICT( project_name, 'sample', 'sam', sample_name, sample_dict, mongoDB_URL = current_app.config['MONGO_URL'] )
        return( True )
    print( "update_sampleVAR_status(): !!! ERROR !!! updating status: {0}, for variant: {1}, in sample: {2}".format( new_status, variant_name, sample_name ))
    return( False )
